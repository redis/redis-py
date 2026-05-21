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
