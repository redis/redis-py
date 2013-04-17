import os
from redis.client import Redis


def from_url(url, db=None, **kwargs):
    """Returns an active Redis client generated from the given database URL.

    Will attempt to extract the database id from the path url fragment, if
    none is provided.
    """
    return Redis.from_url(url, db, **kwargs)


def which(exe, raise_=True):
    "returns the full pathname to exe"
    result = exe
    if not os.path.isabs(exe):
        for sdir in os.environ.get("PATH", "").split(os.path.pathsep):
            pname = os.path.join(sdir, exe)
            if os.path.exists(pname):
                result = pname
                break
    if os.path.exists(result):
        return os.path.normpath(result)
    if raise_ == True:
        raise IOError(u"unable to find %s" % exe)
    elif raise_ in [ False, None, ]:
        return None
    else:
        raise raise_

