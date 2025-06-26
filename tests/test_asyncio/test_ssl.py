from urllib.parse import urlparse
import pytest
import pytest_asyncio
import redis.asyncio as redis

# Skip test or not based on cryptography installation
try:
    import cryptography  # noqa

    skip_if_cryptography = pytest.mark.skipif(False, reason="")
    skip_if_nocryptography = pytest.mark.skipif(False, reason="")
except ImportError:
    skip_if_cryptography = pytest.mark.skipif(True, reason="cryptography not installed")
    skip_if_nocryptography = pytest.mark.skipif(
        True, reason="cryptography not installed"
    )


@pytest.mark.ssl
class TestSSL:
    """Tests for SSL connections in asyncio."""

    @pytest_asyncio.fixture()
    async def _get_client(self, request):
        ssl_url = request.config.option.redis_ssl_url
        p = urlparse(ssl_url)[1].split(":")
        client = redis.Redis(host=p[0], port=p[1], ssl=True)
        yield client
        await client.aclose()

    async def test_ssl_with_invalid_cert(self, _get_client):
        """Test SSL connection with invalid certificate."""
        pass

    async def test_cert_reqs_none_with_check_hostname(self, request):
        """Test that when ssl_cert_reqs=none is used with ssl_check_hostname=True,
        the connection is created successfully with check_hostname internally set to False"""
        ssl_url = request.config.option.redis_ssl_url
        parsed_url = urlparse(ssl_url)
        r = redis.Redis(
            host=parsed_url.hostname,
            port=parsed_url.port,
            ssl=True,
            ssl_cert_reqs="none",
            # Check that ssl_check_hostname is ignored, when ssl_cert_reqs=none
            ssl_check_hostname=True,
        )
        try:
            # Connection should be successful
            assert await r.ping()
            # check_hostname should have been automatically set to False
            assert r.connection_pool.connection_class == redis.SSLConnection
            conn = r.connection_pool.make_connection()
            assert conn.check_hostname is False
        finally:
            await r.aclose()
