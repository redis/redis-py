from urllib.parse import urlparse

import pytest

import redis
from redis.exceptions import ConnectionError


class TestSSL:
    """Tests for SSL connections

    This relies on the --redis-ssl-url purely for rebuilding the client
    and connecting to the appropriate port.
    """

    def test_ssl_with_invalid_cert(self, request):
        ssl_url = request.config.option.redis_ssl_url
        sslclient = redis.from_url(ssl_url)
        with pytest.raises(ConnectionError) as e:
            sslclient.ping()
            assert "SSL: CERTIFICATE_VERIFY_FAILED" in str(e)

    def test_ssl_connection(self, request):
        ssl_url = request.config.option.redis_ssl_url
        p = urlparse(ssl_url)[1].split(":")
        r = redis.Redis(host=p[0], port=p[1], ssl=True, ssl_cert_reqs="none")
        assert r.ping()

    def test_ssl_connection_without_ssl(self, request):
        ssl_url = request.config.option.redis_ssl_url
        p = urlparse(ssl_url)[1].split(":")
        r = redis.Redis(host=p[0], port=p[1], ssl=False)

        with pytest.raises(ConnectionError) as e:
            r.ping()
            assert "Connection closed by server" in str(e)
