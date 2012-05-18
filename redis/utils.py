from urlparse import urlparse

from .client import Redis

DEFAULT_DATABASE_ID = 0

def from_url(url, db=None):
    """Returns an active Redis client generated from the given database URL.

    Will attempt to extract the database id from the path url fragment, if
    none is provided.
    """

    url = urlparse(url)

    # Make sure it's a redis database.
    if url.scheme:
        assert url.scheme == 'redis'

    # Attempt to resolve database id.
    if db is None:
        try:
            db = int(url.path.replace('/', ''))
        except (AttributeError, ValueError):
            db = DEFAULT_DATABASE_ID

    return Redis(
        host=url.hostname,
        port=url.port,
        db=db,
        password=url.password)