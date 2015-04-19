import functools
from contextlib import contextmanager
from redis.exceptions import WatchError


try:
    import hiredis
    HIREDIS_AVAILABLE = True
except ImportError:
    HIREDIS_AVAILABLE = False


def from_url(url, db=None, **kwargs):
    """
    Returns an active Redis client generated from the given database URL.

    Will attempt to extract the database id from the path url fragment, if
    none is provided.
    """
    from redis.client import Redis
    return Redis.from_url(url, db, **kwargs)


def transactional(*watches, **trans_kwargs):
    """
    Convenience function for decorating a callable `func` as executable in a
    transaction while watching all keys specified in `watches`. The 'func'
    callable should expect a Pipeline object as its first argument.
    """
    shard_hint = trans_kwargs.get('shard_hint')
    value_from_callable = trans_kwargs.get('value_from_callable', False)

    def decorator(func):
        @functools.wraps(func)
        def wrapper(redis_obj, *args, **kwargs):
            with redis_obj.pipeline(True, shard_hint) as pipe:
                while 1:
                    try:
                        if watches:
                            pipe.watch(*watches)
                        func_value = func(pipe, *args, **kwargs)
                        exec_value = pipe.execute()
                        if value_from_callable:
                            return func_value
                        else:
                            return exec_value
                    except WatchError:
                        continue
        return wrapper

    return decorator


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
