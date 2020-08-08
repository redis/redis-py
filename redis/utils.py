from contextlib import contextmanager


def from_url(url, db=None, **kwargs):
    """
    Returns an active Redis client generated from the given database URL.

    Will attempt to extract the database id from the path url fragment, if
    none is provided.
    """
    from redis.client import Redis
    return Redis.from_url(url, db, **kwargs)


@contextmanager
def pipeline(redis_obj):
    p = redis_obj.pipeline()
    yield p
    p.execute()


def str_if_bytes(value):
    return (
        value.decode('utf-8', errors='replace')
        if isinstance(value, bytes)
        else value
    )


def safe_str(value):
    return str(str_if_bytes(value))
