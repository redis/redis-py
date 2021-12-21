import os
import socket
import ssl
from urllib.parse import urlparse

import pytest

import redis
from redis.exceptions import ConnectionError, RedisError

from .conftest import skip_if_cryptography, skip_if_nocryptography


@pytest.mark.ssl
class TestSSL:
    """Tests for SSL connections

    This relies on the --redis-ssl-url purely for rebuilding the client
    and connecting to the appropriate port.
    """

    ROOT = os.path.join(os.path.dirname(__file__), "..")
    CERT_DIR = os.path.abspath(os.path.join(ROOT, "docker", "stunnel", "keys"))
    if not os.path.isdir(CERT_DIR):  # github actions package validation case
        CERT_DIR = os.path.abspath(
            os.path.join(ROOT, "..", "docker", "stunnel", "keys")
        )
        if not os.path.isdir(CERT_DIR):
            raise IOError(f"No SSL certificates found. They should be in {CERT_DIR}")

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

    def test_validating_self_signed_certificate(self, request):
        ssl_url = request.config.option.redis_ssl_url
        p = urlparse(ssl_url)[1].split(":")
        r = redis.Redis(
            host=p[0],
            port=p[1],
            ssl=True,
            ssl_certfile=os.path.join(self.CERT_DIR, "server-cert.pem"),
            ssl_keyfile=os.path.join(self.CERT_DIR, "server-key.pem"),
            ssl_cert_reqs="required",
            ssl_ca_certs=os.path.join(self.CERT_DIR, "server-cert.pem"),
        )
        assert r.ping()

    def _create_oscp_conn(self, request):
        ssl_url = request.config.option.redis_ssl_url
        p = urlparse(ssl_url)[1].split(":")
        r = redis.Redis(
            host=p[0],
            port=p[1],
            ssl=True,
            ssl_certfile=os.path.join(self.CERT_DIR, "server-cert.pem"),
            ssl_keyfile=os.path.join(self.CERT_DIR, "server-key.pem"),
            ssl_cert_reqs="required",
            ssl_ca_certs=os.path.join(self.CERT_DIR, "server-cert.pem"),
            ssl_validate_ocsp=True,
        )
        return r

    @skip_if_cryptography()
    def test_ssl_ocsp_called(self, request):
        r = self._create_oscp_conn(request)
        with pytest.raises(RedisError) as e:
            assert r.ping()
            assert "cryptography not installed" in str(e)

    @skip_if_nocryptography()
    def test_ssl_ocsp_called_withcrypto(self, request):
        r = self._create_oscp_conn(request)
        with pytest.raises(ConnectionError) as e:
            assert r.ping()
            assert "No AIA information present in ssl certificate" in str(e)

        # rediss://, url based
        ssl_url = request.config.option.redis_ssl_url
        sslclient = redis.from_url(ssl_url)
        with pytest.raises(ConnectionError) as e:
            sslclient.ping()
            assert "No AIA information present in ssl certificate" in str(e)

    @skip_if_nocryptography()
    def test_valid_ocsp_cert_http(self):
        from redis.ocsp import OCSPVerifier

        hostnames = ["github.com", "aws.amazon.com", "ynet.co.il", "microsoft.com"]
        for hostname in hostnames:
            context = ssl.create_default_context()
            with socket.create_connection((hostname, 443)) as sock:
                with context.wrap_socket(sock, server_hostname=hostname) as wrapped:
                    ocsp = OCSPVerifier(wrapped, hostname, 443)
                    assert ocsp.is_valid()

    @skip_if_nocryptography()
    def test_revoked_ocsp_certificate(self):
        from redis.ocsp import OCSPVerifier

        context = ssl.create_default_context()
        hostname = "revoked.badssl.com"
        with socket.create_connection((hostname, 443)) as sock:
            with context.wrap_socket(sock, server_hostname=hostname) as wrapped:
                ocsp = OCSPVerifier(wrapped, hostname, 443)
                assert ocsp.is_valid() is False

    @skip_if_nocryptography()
    def test_unauthorized_ocsp(self):
        from redis.ocsp import OCSPVerifier

        context = ssl.create_default_context()
        hostname = "stackoverflow.com"
        with socket.create_connection((hostname, 443)) as sock:
            with context.wrap_socket(sock, server_hostname=hostname) as wrapped:
                ocsp = OCSPVerifier(wrapped, hostname, 443)
                with pytest.raises(ConnectionError):
                    ocsp.is_valid()

    @skip_if_nocryptography()
    def test_ocsp_not_present_in_response(self):
        from redis.ocsp import OCSPVerifier

        context = ssl.create_default_context()
        hostname = "google.co.il"
        with socket.create_connection((hostname, 443)) as sock:
            with context.wrap_socket(sock, server_hostname=hostname) as wrapped:
                ocsp = OCSPVerifier(wrapped, hostname, 443)
                assert ocsp.is_valid() is False

    @skip_if_nocryptography()
    def test_unauthorized_then_direct(self):
        from redis.ocsp import OCSPVerifier

        # these certificates on the socket end return unauthorized
        # then the second call succeeds
        hostnames = ["wikipedia.org", "squarespace.com"]
        for hostname in hostnames:
            context = ssl.create_default_context()
            with socket.create_connection((hostname, 443)) as sock:
                with context.wrap_socket(sock, server_hostname=hostname) as wrapped:
                    ocsp = OCSPVerifier(wrapped, hostname, 443)
                    assert ocsp.is_valid()
