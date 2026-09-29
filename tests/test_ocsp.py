import datetime

import pytest

pytest.importorskip("cryptography")

from cryptography import x509  # noqa: E402
from cryptography.hazmat.primitives import hashes, serialization  # noqa: E402
from cryptography.hazmat.primitives.asymmetric import rsa  # noqa: E402
from cryptography.x509 import ocsp  # noqa: E402
from cryptography.x509.oid import NameOID  # noqa: E402

from redis.exceptions import ConnectionError  # noqa: E402
from redis.ocsp import _check_certificate  # noqa: E402


def _key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _cert(subject, issuer_name, key, issuer_key, serial, ca=False):
    now = datetime.datetime.utcnow()
    builder = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, subject)]))
        .issuer_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, issuer_name)]))
        .public_key(key.public_key())
        .serial_number(serial)
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=365))
    )
    if ca:
        builder = builder.add_extension(
            x509.BasicConstraints(ca=True, path_length=None), critical=True
        )
    return builder.sign(issuer_key, hashes.SHA256())


def _good_response(target, issuer, issuer_key):
    now = datetime.datetime.utcnow()
    response = (
        ocsp.OCSPResponseBuilder()
        .add_response(
            cert=target,
            issuer=issuer,
            algorithm=hashes.SHA256(),
            cert_status=ocsp.OCSPCertStatus.GOOD,
            this_update=now - datetime.timedelta(hours=1),
            next_update=now + datetime.timedelta(days=1),
            revocation_time=None,
            revocation_reason=None,
        )
        .responder_id(ocsp.OCSPResponderEncoding.NAME, issuer)
        .sign(issuer_key, hashes.SHA256())
    )
    return response.public_bytes(serialization.Encoding.DER)


def test_check_certificate_accepts_matching_response():
    ca_key = _key()
    ca = _cert("Test CA", "Test CA", ca_key, ca_key, 1, ca=True)
    cert = _cert("node", "Test CA", _key(), ca_key, 111)

    assert _check_certificate(ca, cert, _good_response(cert, ca, ca_key)) is True


def test_check_certificate_rejects_response_for_other_serial():
    # A validly signed GOOD response for a different, non-revoked certificate
    # from the same issuer must not be accepted for the certificate under check.
    ca_key = _key()
    ca = _cert("Test CA", "Test CA", ca_key, ca_key, 1, ca=True)
    cert = _cert("node", "Test CA", _key(), ca_key, 111)
    other = _cert("other", "Test CA", _key(), ca_key, 999)

    with pytest.raises(ConnectionError) as e:
        _check_certificate(ca, cert, _good_response(other, ca, ca_key))
    assert "serial number does not match" in str(e.value)
