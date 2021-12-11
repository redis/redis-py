import pytest

import redis
from redis.exceptions import ConnectionError


class TestSSL:
    """Tests for SSL connections"""

    def test_ssl_with_invalid_cert(self, request):
        ssl_url = request.config.option.redis_ssl_url
        sslclient = redis.from_url(ssl_url)
        with pytest.raises(ConnectionError) as e:
            sslclient.ping()
            assert "SSL: CERTIFICATE_VERIFY_FAILED" in str(e)


#    def test_ssl_connection_creation(self, sslclient):
#        assert sslclient.ping()
