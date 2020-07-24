from urllib.parse import urlparse

from redis import utils


def test_from_url(request):
    redis_url = request.config.getoption("--redis-url")
    parts = urlparse(redis_url)
    host = parts.hostname
    db = int(parts.path.split('/')[1])
    client = utils.from_url(redis_url)
    assert client.connection_pool.connection_kwargs['host'] == host
    assert client.connection_pool.connection_kwargs['db'] == db


def test_pipeline(r):
    with utils.pipeline(r) as p:
        p.ping()
        assert len(p.command_stack) == 1
    assert len(p.command_stack) == 0
