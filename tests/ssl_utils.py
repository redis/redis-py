import enum
import os
from collections import namedtuple

CN_USERNAME = "test_user"
CLIENT_CERT_NAME = "client.crt"
CLIENT_CN_CERT_NAME = f"{CN_USERNAME}.crt"
CLIENT_KEY_NAME = "client.key"
CLIENT_CN_KEY_NAME = f"{CN_USERNAME}.key"
SERVER_CERT_NAME = "redis.crt"
SERVER_KEY_NAME = "redis.key"
CA_CERT_NAME = "ca.crt"


class CertificateType(str, enum.Enum):
    client = "client"
    server = "server"
    client_cn = "client-cn"


TLSFiles = namedtuple("TLSFiles", ["certfile", "keyfile", "ca_certfile"])


def get_tls_certificates(
    subdir: str = "standalone",
    cert_type: CertificateType = CertificateType.client,
):
    root = os.path.join(os.path.dirname(__file__), "..")
    cert_subdir = ("dockers", subdir, "tls")
    cert_dir = os.path.abspath(os.path.join(root, *cert_subdir))
    if not os.path.isdir(cert_dir):  # github actions package validation case
        cert_dir = os.path.abspath(os.path.join(root, "..", *cert_subdir))
        if not os.path.isdir(cert_dir):
            raise OSError(f"No SSL certificates found. They should be in {cert_dir}")

    if cert_type == CertificateType.client:
        return TLSFiles(
            os.path.join(cert_dir, CLIENT_CERT_NAME),
            os.path.join(cert_dir, CLIENT_KEY_NAME),
            os.path.join(cert_dir, CA_CERT_NAME),
        )
    elif cert_type == CertificateType.server:
        return TLSFiles(
            os.path.join(cert_dir, SERVER_CERT_NAME),
            os.path.join(cert_dir, SERVER_KEY_NAME),
            os.path.join(cert_dir, CA_CERT_NAME),
        )
    elif cert_type == CertificateType.client_cn:
        return TLSFiles(
            os.path.join(cert_dir, CLIENT_CN_CERT_NAME),
            os.path.join(cert_dir, CLIENT_CN_KEY_NAME),
            os.path.join(cert_dir, CA_CERT_NAME),
        )
