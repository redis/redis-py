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

    SERVER_CERT = os.path.join(CERT_DIR, "server-cert.pem")
    SERVER_KEY = os.path.join(CERT_DIR, "server-key.pem")

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
            ssl_certfile=self.SERVER_CERT,
            ssl_keyfile=self.SERVER_KEY,
            ssl_cert_reqs="required",
            ssl_ca_certs=self.SERVER_CERT,
        )
        assert r.ping()

    def test_validating_self_signed_string_certificate(self, request):
        with open(self.SERVER_CERT) as f:
            cert_data = f.read()
        ssl_url = request.config.option.redis_ssl_url
        p = urlparse(ssl_url)[1].split(":")
        r = redis.Redis(
            host=p[0],
            port=p[1],
            ssl=True,
            ssl_certfile=self.SERVER_CERT,
            ssl_keyfile=self.SERVER_KEY,
            ssl_cert_reqs="required",
            ssl_ca_data=cert_data,
        )
        assert r.ping()

    def _create_oscp_conn(self, request):
        ssl_url = request.config.option.redis_ssl_url
        p = urlparse(ssl_url)[1].split(":")
        r = redis.Redis(
            host=p[0],
            port=p[1],
            ssl=True,
            ssl_certfile=self.SERVER_CERT,
            ssl_keyfile=self.SERVER_KEY,
            ssl_cert_reqs="required",
            ssl_ca_certs=self.SERVER_CERT,
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

        hostnames = ["github.com", "aws.amazon.com", "ynet.co.il"]
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
                with pytest.raises(ConnectionError) as e:
                    assert ocsp.is_valid()
                    assert "REVOKED" in str(e)

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
                with pytest.raises(ConnectionError) as e:
                    assert ocsp.is_valid()
                    assert "from the" in str(e)

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

    @skip_if_nocryptography()
    def test_mock_ocsp_staple(self, request):
        import OpenSSL

        ssl_url = request.config.option.redis_ssl_url
        p = urlparse(ssl_url)[1].split(":")
        r = redis.Redis(
            host=p[0],
            port=p[1],
            ssl=True,
            ssl_certfile=self.SERVER_CERT,
            ssl_keyfile=self.SERVER_KEY,
            ssl_cert_reqs="required",
            ssl_ca_certs=self.SERVER_CERT,
            ssl_validate_ocsp=True,
            ssl_ocsp_context=p,  # just needs to not be none
        )

        with pytest.raises(RedisError):
            r.ping()

        ctx = OpenSSL.SSL.Context(OpenSSL.SSL.SSLv23_METHOD)
        ctx.use_certificate_file(self.SERVER_CERT)
        ctx.use_privatekey_file(self.SERVER_KEY)

        r = redis.Redis(
            host=p[0],
            port=p[1],
            ssl=True,
            ssl_certfile=self.SERVER_CERT,
            ssl_keyfile=self.SERVER_KEY,
            ssl_cert_reqs="required",
            ssl_ca_certs=self.SERVER_CERT,
            ssl_ocsp_context=ctx,
            ssl_ocsp_expected_cert=open(self.SERVER_KEY, "rb").read(),
            ssl_validate_ocsp_stapled=True,
        )

        with pytest.raises(ConnectionError) as e:
            r.ping()
            assert "no ocsp response present" in str(e)

        r = redis.Redis(
            host=p[0],
            port=p[1],
            ssl=True,
            ssl_certfile=self.SERVER_CERT,
            ssl_keyfile=self.SERVER_KEY,
            ssl_cert_reqs="required",
            ssl_ca_certs=self.SERVER_CERT,
            ssl_validate_ocsp_stapled=True,
        )

        with pytest.raises(ConnectionError) as e:
            r.ping()
            assert "no ocsp response present" in str(e)
