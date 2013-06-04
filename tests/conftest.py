import pytest
import redis


def _get_client(cls, request=None):
    client = cls(host='localhost', port=6379, db=9)
    client.flushdb()
    if request:
        request.addfinalizer(client.flushdb)
    return client


def skip_if_server_version_lt(min_version):
    version = _get_client(redis.Redis).info()['redis_version']
    c = "StrictVersion('%s') < StrictVersion('%s')" % (version, min_version)
    return pytest.mark.skipif(c)


@pytest.fixture()
def r(request):
    return _get_client(redis.Redis, request)


@pytest.fixture()
def sr(request):
    return _get_client(redis.StrictRedis, request)
