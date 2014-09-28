from contextlib import contextmanager
from urlparse import urlparse


try:
    import hiredis
    HIREDIS_AVAILABLE = True
except ImportError:
    HIREDIS_AVAILABLE = False


def from_url(url, db=None, master_for_args={}, **kwargs):
    """
    Returns an active Redis client generated from the given database URL.

    Will attempt to extract the database id from the path url fragment, if
    none is provided.

    When url is starting with sentinels://, a sentinel object will be returned
    if no query parameter service_name in url, otherwise the master redis
    client will be returned. master_for_args will be applied to
    Sentinel.master_for.
    >>> import redis
    >>> redis.from_url('sentinels://node1:17700,node2:17700')
    Sentinel<sentinels=[node1:17700,node2:17700]>

    >>> redis.from_url(
        'sentinels://node1:17700,node2:17700?service_name=mymaster', db=1)
    StrictRedis<SentinelConnectionPool<service=mymaster(master)>

    >>> redis.from_url(
       'sentinels://node1:17700,node2:17700?service_name=mymaster&db=3', db=1)
    StrictRedis<SentinelConnectionPool<service=mymaster(master)>

    >>> redis.from_url(
        'sentinels://node1:17700,node2:17700?service_name=mymaster',
        db=1,
        master_for_args={'redis_class':redis.Redis})
    Redis<SentinelConnectionPool<service=mymaster(master)>
    """
    parse_result = urlparse(url)
    if parse_result.scheme == 'sentinels':
        from sentinel import Sentinel
        sentinel, db_from_url, service_name = Sentinel.from_url(url, **kwargs)

        if not service_name:
            return sentinel
        else:
            return sentinel.master_for(service_name,
                                       db=(db_from_url or db or 0),
                                       **master_for_args)
    from redis.client import Redis
    return Redis.from_url(url, db, **kwargs)


@contextmanager
def pipeline(redis_obj):
    p = redis_obj.pipeline()
    yield p
    p.execute()


class dummy(object):
    """
    Instances of this class can be used as an attribute container.
    """
    pass
