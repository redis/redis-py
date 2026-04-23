import copy
import random
import string
from typing import TYPE_CHECKING, Any, Callable, Iterable, List, Mapping, Tuple

import redis
from redis.typing import ChannelT, KeysT, KeyT

if TYPE_CHECKING:
    from redis._parsers import Encoder


def list_or_args(keys: KeysT, args: Tuple[KeyT, ...]) -> List[KeyT]:
    # returns a single new list combining keys and args
    try:
        iter(keys)
        # a string or bytes instance can be iterated, but indicates
        # keys wasn't passed as a list
        if isinstance(keys, (bytes, str)):
            keys = [keys]
        else:
            keys = list(keys)
    except TypeError:
        keys = [keys]
    if args:
        keys.extend(args)
    return keys


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
            except ValueError:
                try:
                    res.append(float(item))
                except ValueError:
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


def partition_pubsub_subscriptions_by_handler(
    subscriptions: Mapping[ChannelT, Callable | None],
    encoder: "Encoder",
) -> tuple[list[ChannelT], dict[str, Callable]]:
    """Partition a PubSub ``{name: handler|None}`` mapping into the positional
    and keyword arguments expected by ``[s|p]subscribe``.

    For python3, we can't pass bytestrings as keyword arguments, so names
    with a handler are decoded (keyword args). Names subscribed without a
    callback are stored with a ``None`` handler and may have binary values
    that are not valid in the current encoding (e.g. arbitrary bytes that
    are not valid UTF-8); they are returned as raw keys (positional args)
    so that no decoding is required.
    """
    subscriptions_without_handlers: list[ChannelT] = []
    subscriptions_with_handlers: dict[str, Callable] = {}
    for k, v in subscriptions.items():
        if v is not None:
            subscriptions_with_handlers[encoder.decode(k, force=True)] = v
        else:
            subscriptions_without_handlers.append(k)
    return subscriptions_without_handlers, subscriptions_with_handlers
