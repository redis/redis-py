import sys
from contextlib import contextmanager


try:
    import hiredis  # noqa
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


def random_choices(population, weights):
    """
    randomly by weight
    """
    if sys.version_info[0] == 3 and sys.version_info[1] >= 6:
        from random import choices
        return choices(population, weights, k=len(population))
    else:
        return population
