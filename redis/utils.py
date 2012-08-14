from redis.client import Redis

def from_url(url, db=None, **kwargs):
    """Returns an active Redis client generated from the given database URL.

    Will attempt to extract the database id from the path url fragment, if
    none is provided.
    """
    return Redis.from_url(url, db, **kwargs)
