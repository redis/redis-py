import pytest
import redis


def _get_client(cls, request=None, **kwargs):
    params = {'host': 'localhost', 'port': 6379, 'db': 9}
    params.update(kwargs)
    client = cls(**params)
    client.flushdb()
    if request:
        request.addfinalizer(client.flushdb)
    return client


def skip_if_server_version_lt(min_version):
    version = _get_client(redis.Redis).info()['redis_version']
    c = "StrictVersion('%s') < StrictVersion('%s')" % (version, min_version)
    return pytest.mark.skipif(c)


@pytest.fixture()
def r(request, **kwargs):
    return _get_client(redis.Redis, request, **kwargs)


@pytest.fixture()
def sr(request, **kwargs):
    return _get_client(redis.StrictRedis, request, **kwargs)
