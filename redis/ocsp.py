import base64
import ssl
from urllib.parse import urljoin, urlparse

import cryptography.hazmat.primitives.hashes
import requests
from cryptography import hazmat, x509
from cryptography.hazmat import backends
from cryptography.x509 import ocsp

from redis.exceptions import ConnectionError


class OCSPVerifier:
    """A class to verify ssl sockets for RFC6960, the
    Online Certificate Status Protocol.

    @see https://datatracker.ietf.org/doc/html/rfc6960
    """

    def __init__(self, sock):
        self.SOCK = sock

    def _bin2ascii(self, der):
        """Convert SSL certificates in a binary (DER) format to ASCII PEM."""
        pem = ssl.DER_cert_to_PEM_cert(der)
        cert = x509.load_pem_x509_certificate(pem.encode(), backends.default_backend())
        return cert

    def get_certificate_components(self):
        """This function returns the certificate, primary issuer, and primary ocsp server
        in the chain for a socket already wrapped with ssl.
        """

        # convert the binary certifcate to text
        der = self.SOCK.getpeercert(True)
        cert = self._bin2ascii(der)

        aia = cert.extensions.get_extension_for_oid(
            x509.oid.ExtensionOID.AUTHORITY_INFORMATION_ACCESS
        ).value

        # fetch certificate issuers
        issuers = [
            i
            for i in aia
            if i.access_method == x509.oid.AuthorityInformationAccessOID.CA_ISSUERS
        ]
        try:
            issuer = issuers[0].access_location.value
        except IndexError:
            raise ConnectionError("no issuers in certificate")

        # now, the series of ocsp server entries
        ocsps = [
            i
            for i in aia
            if i.access_method == x509.oid.AuthorityInformationAccessOID.OCSP
        ]
        try:
            ocsp = ocsps[0].access_location.value
        except IndexError:
            raise ConnectionError("no ocsp servers in certificate")

        return cert, issuer, ocsp

    def build_certificate_url(self, server, cert, issuer_cert):
        """Return the complete url to the ocsp"""
        orb = ocsp.OCSPRequestBuilder()

        # add_certificate returns an initialized OCSPRequestBuilder
        orb = orb.add_certificate(
            cert, issuer_cert, cryptography.hazmat.primitives.hashes.SHA256()
        )
        request = orb.build()

        path = base64.b64encode(
            request.public_bytes(hazmat.primitives.serialization.Encoding.DER)
        )
        url = urljoin(server, path.decode())
        return url

    def check_certificate(self, server, cert, issuer_cert):
        """Checks the validitity of an ocsp server for an issuer"""
        ocsp_url = self.build_certificate_url(server, cert, issuer_cert)

        # HTTP 1.1 mandates the addition of the Host header in ocsp responses
        header = {"Host": urlparse(ocsp_url).netloc}
        r = requests.get(ocsp_url, headers=header)
        if not r.ok:
            raise ConnectionError("failed to fetch ocsp certificate")

        ocsp_response = ocsp.load_der_ocsp_response(r.content)
        if ocsp_response.response_status == ocsp.OCSPResponseStatus.UNAUTHORIZED:
            raise ConnectionError(
                "you are not authorized to view this ocsp certificate"
            )
        if ocsp_response.response_status == ocsp.OCSPResponseStatus.SUCCESSFUL:
            if ocsp_response.certificate_status == ocsp.OCSPCertStatus.REVOKED:
                return False
            else:
                return True
        else:
            return False

    def is_valid(self):
        """Returns the validity of the certificate wrapping our socket"""
        cert, issuer_url, ocsp_server = self.get_certificate_components()

        # now get the ascii formatted issuer certificate
        r = requests.get(issuer_url)
        if not r.ok:
            raise ConnectionError("failed to fetch issuer certificate")
        der = r.content
        issuer_cert = self._bin2ascii(der)

        # validate the certificate
        return self.check_certificate(ocsp_server, cert, issuer_cert)
