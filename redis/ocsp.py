import base64
import ssl
from urllib.parse import urljoin, urlparse

import cryptography.hazmat.primitives.hashes
import requests
from cryptography import hazmat, x509
from cryptography.hazmat import backends
from cryptography.x509 import ocsp

from redis.exceptions import AuthorizationError, ConnectionError


class OCSPVerifier:
    """A class to verify ssl sockets for RFC6960/RFC6961.

    @see https://datatracker.ietf.org/doc/html/rfc6960
    @see https://datatracker.ietf.org/doc/html/rfc6961
    """

    def __init__(self, sock, host, port, ca_certs=None):
        self.SOCK = sock
        self.HOST = host
        self.PORT = port
        self.CA_CERTS = ca_certs

    def _bin2ascii(self, der):
        """Convert SSL certificates in a binary (DER) format to ASCII PEM."""

        pem = ssl.DER_cert_to_PEM_cert(der)
        cert = x509.load_pem_x509_certificate(pem.encode(), backends.default_backend())
        return cert

    def components_from_socket(self):
        """This function returns the certificate, primary issuer, and primary ocsp server
        in the chain for a socket already wrapped with ssl.
        """

        # convert the binary certifcate to text
        der = self.SOCK.getpeercert(True)
        if der is False:
            raise ConnectionError("no certificate found for ssl peer")
        cert = self._bin2ascii(der)
        return self._certificate_components(cert)

    def _certificate_components(self, cert):
        """Given an SSL certificate, retract the useful components for
        validating the certificate status with an OCSP server.

        Args:
            cert ([bytes]): A PEM encoded ssl certificate
        """

        try:
            aia = cert.extensions.get_extension_for_oid(
                x509.oid.ExtensionOID.AUTHORITY_INFORMATION_ACCESS
            ).value
        except cryptography.x509.extensions.ExtensionNotFound:
            raise ConnectionError("No AIA information present in ssl certificate")

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

    def components_from_direct_connection(self):
        """Return the certificate, primary issuer, and primary ocsp server
        from the host defined by the socket. This is useful in cases where
        different certificates are occasionally presented.
        """

        pem = ssl.get_server_certificate((self.HOST, self.PORT), ca_certs=self.CA_CERTS)
        cert = x509.load_pem_x509_certificate(pem.encode(), backends.default_backend())
        return self._certificate_components(cert)

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
        url = urljoin(server, path.decode("ascii"))
        return url

    def check_certificate(self, server, cert, issuer_url):
        """Checks the validitity of an ocsp server for an issuer"""

        r = requests.get(issuer_url)
        if not r.ok:
            raise ConnectionError("failed to fetch issuer certificate")
        der = r.content
        issuer_cert = self._bin2ascii(der)

        ocsp_url = self.build_certificate_url(server, cert, issuer_cert)

        # HTTP 1.1 mandates the addition of the Host header in ocsp responses
        header = {
            "Host": urlparse(ocsp_url).netloc,
            "Content-Type": "application/ocsp-request",
        }
        r = requests.get(ocsp_url, headers=header)
        if not r.ok:
            raise ConnectionError("failed to fetch ocsp certificate")

        ocsp_response = ocsp.load_der_ocsp_response(r.content)
        if ocsp_response.response_status == ocsp.OCSPResponseStatus.UNAUTHORIZED:
            raise AuthorizationError(
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
        """Returns the validity of the certificate wrapping our socket.
        This first retrieves for validate the certificate, issuer_url,
        and ocsp_server for certificate validate. Then retrieves the
        issuer certificate from the issuer_url, and finally checks
        the valididy of OCSP revocation status.
        """

        # validate the certificate
        try:
            cert, issuer_url, ocsp_server = self.components_from_socket()
            return self.check_certificate(ocsp_server, cert, issuer_url)
        except AuthorizationError:
            cert, issuer_url, ocsp_server = self.components_from_direct_connection()
            return self.check_certificate(ocsp_server, cert, issuer_url)
