import ssl
import unittest.mock
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

    async def test_ssl_flags_applied_to_context(self, request):
        """
        Test that ssl_include_verify_flags and ssl_exclude_verify_flags
        are properly applied to the SSL context
        """
        ssl_url = request.config.option.redis_ssl_url
        parsed_url = urlparse(ssl_url)

        # Test with specific SSL verify flags
        ssl_include_verify_flags = [
            ssl.VerifyFlags.VERIFY_CRL_CHECK_LEAF,  # Disable strict verification
            ssl.VerifyFlags.VERIFY_CRL_CHECK_CHAIN,  # Enable partial chain
        ]

        ssl_exclude_verify_flags = [
            ssl.VerifyFlags.VERIFY_X509_STRICT,  # Disable trusted first
        ]

        r = redis.Redis(
            host=parsed_url.hostname,
            port=parsed_url.port,
            ssl=True,
            ssl_cert_reqs="none",
            ssl_include_verify_flags=ssl_include_verify_flags,
            ssl_exclude_verify_flags=ssl_exclude_verify_flags,
        )

        try:
            # Get the connection to trigger SSL context creation
            conn = r.connection_pool.make_connection()
            assert isinstance(conn, redis.SSLConnection)

            # Verify the flags were processed by checking they're stored in connection
            assert conn.include_verify_flags is not None
            assert len(conn.include_verify_flags) == 2

            assert conn.exclude_verify_flags is not None
            assert len(conn.exclude_verify_flags) == 1

            # Check each flag individually
            for flag in ssl_include_verify_flags:
                assert flag in conn.include_verify_flags, (
                    f"Flag {flag} not found in stored ssl_include_verify_flags"
                )
            for flag in ssl_exclude_verify_flags:
                assert flag in conn.exclude_verify_flags, (
                    f"Flag {flag} not found in stored ssl_exclude_verify_flags"
                )

            # Test the actual SSL context created by the connection's RedisSSLContext
            # We need to mock the ssl.create_default_context to capture the context
            captured_context = None
            original_create_default_context = ssl.create_default_context

            def capture_context_create_default():
                nonlocal captured_context
                captured_context = original_create_default_context()
                return captured_context

            with unittest.mock.patch(
                "ssl.create_default_context", capture_context_create_default
            ):
                # Trigger SSL context creation by calling get() on the RedisSSLContext
                ssl_context = conn.ssl_context.get()

                # Validate that we captured a context and it has the correct flags applied
                assert captured_context is not None, "SSL context was not captured"
                assert ssl_context is captured_context, (
                    "Returned context should be the captured one"
                )

                # Verify that VERIFY_X509_STRICT was disabled (bit cleared)
                assert not (
                    captured_context.verify_flags & ssl.VerifyFlags.VERIFY_X509_STRICT
                ), "VERIFY_X509_STRICT should be disabled but is enabled"

                # Verify that VERIFY_CRL_CHECK_CHAIN was enabled (bit set)
                assert (
                    captured_context.verify_flags
                    & ssl.VerifyFlags.VERIFY_CRL_CHECK_CHAIN
                ), "VERIFY_CRL_CHECK_CHAIN should be enabled but is disabled"

        finally:
            await r.aclose()
