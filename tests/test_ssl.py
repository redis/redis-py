import socket
import ssl
from urllib.parse import urlparse

import pytest
import redis
from redis.exceptions import ConnectionError, RedisError

from .conftest import skip_if_cryptography, skip_if_nocryptography
from .ssl_utils import get_ssl_filename


@pytest.mark.ssl
class TestSSL:
    """Tests for SSL connections

    This relies on the --redis-ssl-url purely for rebuilding the client
    and connecting to the appropriate port.
    """

    CA_CERT = get_ssl_filename("ca-cert.pem")
    CLIENT_CERT = get_ssl_filename("client-cert.pem")
    CLIENT_KEY = get_ssl_filename("client-key.pem")
    SERVER_CERT = get_ssl_filename("server-cert.pem")

    def test_ssl_with_invalid_cert(self, request):
        ssl_url = request.config.option.redis_ssl_url
        sslclient = redis.from_url(ssl_url)
        with pytest.raises(ConnectionError) as e:
            sslclient.ping()
        assert "SSL: CERTIFICATE_VERIFY_FAILED" in str(e)
        sslclient.close()

    def test_ssl_connection(self, request):
        ssl_url = request.config.option.redis_ssl_url
        p = urlparse(ssl_url)[1].split(":")
        r = redis.Redis(host=p[0], port=p[1], ssl=True, ssl_cert_reqs="none")
        assert r.ping()
        r.close()

    def test_ssl_connection_without_ssl(self, request):
        ssl_url = request.config.option.redis_ssl_url
        p = urlparse(ssl_url)[1].split(":")
        r = redis.Redis(host=p[0], port=p[1], ssl=False)

        with pytest.raises(ConnectionError) as e:
            r.ping()
        assert "Connection closed by server" in str(e)
        r.close()

    def test_validating_self_signed_certificate(self, request):
        ssl_url = request.config.option.redis_ssl_url
        p = urlparse(ssl_url)[1].split(":")
        r = redis.Redis(
            host=p[0],
            port=p[1],
            ssl=True,
            ssl_certfile=self.CLIENT_CERT,
            ssl_keyfile=self.CLIENT_KEY,
            ssl_cert_reqs="required",
            ssl_ca_certs=self.CA_CERT,
        )
        assert r.ping()
        r.close()

    def test_validating_self_signed_string_certificate(self, request):
        with open(self.CA_CERT) as f:
            cert_data = f.read()
        ssl_url = request.config.option.redis_ssl_url
        p = urlparse(ssl_url)[1].split(":")
        r = redis.Redis(
            host=p[0],
            port=p[1],
            ssl=True,
            ssl_certfile=self.CLIENT_CERT,
            ssl_keyfile=self.CLIENT_KEY,
            ssl_cert_reqs="required",
            ssl_ca_data=cert_data,
        )
        assert r.ping()
        r.close()

    @pytest.mark.parametrize(
        "ssl_ciphers",
        [
            "AES256-SHA:DHE-RSA-AES256-SHA:AES128-SHA:DHE-RSA-AES128-SHA",
            "DHE-RSA-AES256-GCM-SHA384",
            "ECDHE-RSA-AES256-SHA384:ECDHE-ECDSA-CHACHA20-POLY1305",
        ],
    )
    def test_ssl_connection_tls12_custom_ciphers(self, request, ssl_ciphers):
        ssl_url = request.config.option.redis_ssl_url
        p = urlparse(ssl_url)[1].split(":")
        r = redis.Redis(
            host=p[0],
            port=p[1],
            ssl=True,
            ssl_cert_reqs="none",
            ssl_min_version=ssl.TLSVersion.TLSv1_3,
            ssl_ciphers=ssl_ciphers,
        )
        assert r.ping()
        r.close()

    def test_ssl_connection_tls12_custom_ciphers_invalid(self, request):
        ssl_url = request.config.option.redis_ssl_url
        p = urlparse(ssl_url)[1].split(":")
        r = redis.Redis(
            host=p[0],
            port=p[1],
            ssl=True,
            ssl_cert_reqs="none",
            ssl_min_version=ssl.TLSVersion.TLSv1_2,
            ssl_ciphers="foo:bar",
        )
        with pytest.raises(RedisError) as e:
            r.ping()
        assert "No cipher can be selected" in str(e)
        r.close()

    @pytest.mark.parametrize(
        "ssl_ciphers",
        [
            "TLS_CHACHA20_POLY1305_SHA256",
            "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256",
        ],
    )
    def test_ssl_connection_tls13_custom_ciphers(self, request, ssl_ciphers):
        # TLSv1.3 does not support changing the ciphers
        ssl_url = request.config.option.redis_ssl_url
        p = urlparse(ssl_url)[1].split(":")
        r = redis.Redis(
            host=p[0],
            port=p[1],
            ssl=True,
            ssl_cert_reqs="none",
            ssl_min_version=ssl.TLSVersion.TLSv1_2,
            ssl_ciphers=ssl_ciphers,
        )
        with pytest.raises(RedisError) as e:
            r.ping()
        assert "No cipher can be selected" in str(e)
        r.close()

    def _create_oscp_conn(self, request):
        ssl_url = request.config.option.redis_ssl_url
        p = urlparse(ssl_url)[1].split(":")
        r = redis.Redis(
            host=p[0],
            port=p[1],
            ssl=True,
            ssl_certfile=self.CLIENT_CERT,
            ssl_keyfile=self.CLIENT_KEY,
            ssl_cert_reqs="required",
            ssl_ca_certs=self.CA_CERT,
            ssl_validate_ocsp=True,
        )
        return r

    @skip_if_cryptography()
    def test_ssl_ocsp_called(self, request):
        r = self._create_oscp_conn(request)
        with pytest.raises(RedisError) as e:
            r.ping()
        assert "cryptography is not installed" in str(e)
        r.close()

    @skip_if_nocryptography()
    def test_ssl_ocsp_called_withcrypto(self, request):
        r = self._create_oscp_conn(request)
        with pytest.raises(ConnectionError) as e:
            assert r.ping()
        assert "No AIA information present in ssl certificate" in str(e)
        r.close()

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
            ssl_certfile=self.CLIENT_CERT,
            ssl_keyfile=self.CLIENT_KEY,
            ssl_cert_reqs="required",
            ssl_ca_certs=self.CA_CERT,
            ssl_validate_ocsp=True,
            ssl_ocsp_context=p,  # just needs to not be none
        )

        with pytest.raises(RedisError):
            r.ping()
        r.close()

        ctx = OpenSSL.SSL.Context(OpenSSL.SSL.SSLv23_METHOD)
        ctx.use_certificate_file(self.CLIENT_CERT)
        ctx.use_privatekey_file(self.CLIENT_KEY)

        r = redis.Redis(
            host=p[0],
            port=p[1],
            ssl=True,
            ssl_certfile=self.CLIENT_CERT,
            ssl_keyfile=self.CLIENT_KEY,
            ssl_cert_reqs="required",
            ssl_ca_certs=self.CA_CERT,
            ssl_ocsp_context=ctx,
            ssl_ocsp_expected_cert=open(self.SERVER_CERT, "rb").read(),
            ssl_validate_ocsp_stapled=True,
        )

        with pytest.raises(ConnectionError) as e:
            r.ping()
        assert "no ocsp response present" in str(e)
        r.close()

        r = redis.Redis(
            host=p[0],
            port=p[1],
            ssl=True,
            ssl_certfile=self.CLIENT_CERT,
            ssl_keyfile=self.CLIENT_KEY,
            ssl_cert_reqs="required",
            ssl_ca_certs=self.CA_CERT,
            ssl_validate_ocsp_stapled=True,
        )

        with pytest.raises(ConnectionError) as e:
            r.ping()
        assert "no ocsp response present" in str(e)
        r.close()
