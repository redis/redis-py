import logging
import sys
from contextlib import contextmanager
from functools import wraps
from typing import Any, Dict, Mapping, Union

try:
    import hiredis  # noqa

    # Only support Hiredis >= 1.0:
    HIREDIS_AVAILABLE = not hiredis.__version__.startswith("0.")
    HIREDIS_PACK_AVAILABLE = hasattr(hiredis, "pack_command")
except ImportError:
    HIREDIS_AVAILABLE = False
    HIREDIS_PACK_AVAILABLE = False

try:
    import ssl  # noqa

    SSL_AVAILABLE = True
except ImportError:
    SSL_AVAILABLE = False

try:
    import cryptography  # noqa

    CRYPTOGRAPHY_AVAILABLE = True
except ImportError:
    CRYPTOGRAPHY_AVAILABLE = False

if sys.version_info >= (3, 8):
    from importlib import metadata
else:
    import importlib_metadata as metadata


def from_url(url, **kwargs):
    """
    Returns an active Redis client generated from the given database URL.

    Will attempt to extract the database id from the path url fragment, if
    none is provided.
    """
    from redis.client import Redis

    return Redis.from_url(url, **kwargs)


@contextmanager
def pipeline(redis_obj):
    p = redis_obj.pipeline()
    yield p
    p.execute()


def str_if_bytes(value: Union[str, bytes]) -> str:
    return (
        value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value
    )


def safe_str(value):
    return str(str_if_bytes(value))


def dict_merge(*dicts: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Merge all provided dicts into 1 dict.
    *dicts : `dict`
        dictionaries to merge
    """
    merged = {}

    for d in dicts:
        merged.update(d)

    return merged


def list_keys_to_dict(key_list, callback):
    return dict.fromkeys(key_list, callback)


def merge_result(command, res):
    """
    Merge all items in `res` into a list.

    This command is used when sending a command to multiple nodes
    and the result from each node should be merged into a single list.

    res : 'dict'
    """
    result = set()

    for v in res.values():
        for value in v:
            result.add(value)

    return list(result)


def warn_deprecated(name, reason="", version="", stacklevel=2):
    import warnings

    msg = f"Call to deprecated {name}."
    if reason:
        msg += f" ({reason})"
    if version:
        msg += f" -- Deprecated since version {version}."
    warnings.warn(msg, category=DeprecationWarning, stacklevel=stacklevel)


def deprecated_function(reason="", version="", name=None):
    """
    Decorator to mark a function as deprecated.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            warn_deprecated(name or func.__name__, reason, version, stacklevel=3)
            return func(*args, **kwargs)

        return wrapper

    return decorator


def _set_info_logger():
    """
    Set up a logger that log info logs to stdout.
    (This is used by the default push response handler)
    """
    if "push_response" not in logging.root.manager.loggerDict.keys():
        logger = logging.getLogger("push_response")
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        logger.addHandler(handler)


def get_lib_version():
    try:
        libver = metadata.version("redis")
    except metadata.PackageNotFoundError:
        libver = "99.99.99"
    return libver


DEFAULT_BLACKLIST = [
    "FT.AGGREGATE",
    "FT.ALIASADD",
    "FT.ALIASDEL",
    "FT.ALIASUPDATE",
    "FT.CURSOR",
    "FT.EXPLAIN",
    "FT.EXPLAINCLI",
    "FT.GET",
    "FT.INFO",
    "FT.MGET",
    "FT.PROFILE",
    "FT.SEARCH",
    "FT.SPELLCHECK",
    "FT.SUGGET",
    "FT.SUGLEN",
    "FT.SYNDUMP",
    "FT.TAGVALS",
    "FT._ALIASADDIFNX",
    "BF.CARD",
    "BF.DEBUG",
    "BF.EXISTS",
    "BF.INFO",
    "BF.MEXISTS",
    "BF.SCANDUMP",
    "CF.COMPACT",
    "CF.COUNT",
    "CF.DEBUG",
    "CF.EXISTS",
    "CF.INFO",
    "CF.MEXISTS",
    "CF.SCANDUMP",
    "CMS.INFO",
    "CMS.QUERY",
    "EXPIRETIME",
    "HRANDFIELD",
    "JSON.DEBUG",
    "PEXPIRETIME",
    "PFCOUNT",
    "PTTL",
    "SRANDMEMBER",
    "TDIGEST.BYRANK",
    "TDIGEST.BYREVRANK",
    "TDIGEST.CDF",
    "TDIGEST.INFO",
    "TDIGEST.MAX",
    "TDIGEST.MIN",
    "TDIGEST.QUANTILE",
    "TDIGEST.RANK",
    "TDIGEST.REVRANK",
    "TDIGEST.TRIMMED_MEAN",
    "TOPK.INFO",
    "TOPK.LIST",
    "TOPK.QUERY",
    "TTL",
]


DEFAULT_WHITELIST = [
    "BITCOUNT",
    "BITFIELD_RO",
    "BITPOS",
    "DBSIZE",
    "DUMP",
    "EVALSHA_RO",
    "EVAL_RO",
    "EXISTS",
    "EXPIRETIME",
    "FCALL_RO",
    "GEODIST",
    "GEOHASH",
    "GEOPOS",
    "GEORADIUSBYMEMBER_RO",
    "GEORADIUS_RO",
    "GEOSEARCH",
    "GET",
    "GETBIT",
    "GETRANGE",
    "HEXISTS",
    "HGET",
    "HGETALL",
    "HKEYS",
    "HLEN",
    "HMGET",
    "HRANDFIELD",
    "HSCAN",
    "HSTRLEN",
    "HVALS",
    "KEYS",
    "LCS",
    "LINDEX",
    "LLEN",
    "LOLWUT",
    "LPOS",
    "LRANGE",
    "MEMORY USAGE",
    "MGET",
    "OBJECT ENCODING",
    "OBJECT FREQ",
    "OBJECT IDLETIME",
    "OBJECT REFCOUNT",
    "PEXPIRETIME",
    "PFCOUNT",
    "PTTL",
    "RANDOMKEY",
    "SCAN",
    "SCARD",
    "SDIFF",
    "SINTER",
    "SINTERCARD",
    "SISMEMBER",
    "SMEMBERS",
    "SMISMEMBER",
    "SORT_RO",
    "SRANDMEMBER",
    "SSCAN",
    "STRLEN",
    "SUBSTR",
    "SUNION",
    "TOUCH",
    "TTL",
    "TYPE",
    "XINFO CONSUMERS",
    "XINFO GROUPS",
    "XINFO STREAM",
    "XLEN",
    "XPENDING",
    "XRANGE",
    "XREAD",
    "XREVRANGE",
    "ZCARD",
    "ZCOUNT",
    "ZDIFF",
    "ZINTER",
    "ZINTERCARD",
    "ZLEXCOUNT",
    "ZMSCORE",
    "ZRANDMEMBER",
    "ZRANGE",
    "ZRANGEBYLEX",
    "ZRANGEBYSCORE",
    "ZRANK",
    "ZREVRANGE",
    "ZREVRANGEBYLEX",
    "ZREVRANGEBYSCORE",
    "ZREVRANK",
    "ZSCAN",
    "ZSCORE",
    "ZUNION",
]
