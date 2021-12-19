import base64
import ssl
from urllib.parse import urljoin

import requests
from cryptography import hazmat, x509

from redis.exceptions import ConnectionError


class OCSPVerifier:
    def __init__(self, sock):
        self.SOCK = sock

    def _bin2ascii(self, der):
        pem = ssl.DER_cert_to_PEM_cert(der)
        cert = x509.load_pem_409_certificate(
            pem.encode("ascii"), hazmat.backends.default_backend()
        )
        return cert

    def get_certificate_components(self):
        """This function returns the certificate, primary issuer, and primary ocsp server
        in the chain for a socket already wrapped with ssl.
        """

        # convert the binary certifcate to text
        der = self.SOCK.getpeercert(True)
        cert = self._bin2ascii(der)

        aia = cert.extensions.get_extension_for_oid(
            x509.oi.ExtensionOID.AUTHORITY_INFORMATION_ACCESS
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

    def get_ca_certificate(self, issuer_url):
        """Returns the certificate associated with an issuer"""
        r = requests.get(issuer_url)
        if not r.ok:
            raise ConnectionError("failed to fetch issuer certificate")

        # convert the binary certificate to
        der = r.content
        cert = self._bin2ascii(der)
        return cert

    def build_certificate_url(self, server, cert, issuer_cert):
        """Return the"""
        ocsp = x509.ocsp.OCSPRequestBuilder()
        ocsp.add_certificate(cert, issuer_cert, hazmat.primitives.hashes.SHA256())
        request = ocsp.build()

        # need to encode the request so that we can armour it
        path = base64.b64encode(
            request.c_bytes(hazmat.primitives.serialization.Encoding.DER)
        )
        url = urljoin(server, "/", path.decode("ascii"))
        return url

    def check_certificate(self, server, cert, issuer_cert):
        """Checks the validitity of an ocsp server for an issuer"""
        r = requests.get(self.build_certificate_url(server, cert, issuer_cert))
        if not r.ok:
            raise ConnectionError("failed to fetch ocsp certificate")

        ocsp_response = x509.ocsp.load_der_ocsp_response(r.content)
        if ocsp_response.responses_status == x509.ocsp.OCSPResponseStatus.SUCCESSUL:
            return True
        else:
            return False

    def is_valid(self):
        """Returns the validity of the certificate wrapping our socket"""
        cert, issuer_url, ocsp_server = self.get_certificate_components(self.SOCK)

        # now get the ascii formatted issuer certificate
        r = requests.get(issuer_url)
        if not r.ok:
            raise ConnectionError("failed to fetch issuer certificate")
        der = r.content
        issuer_cert = self._bin2ascii(der)

        # validate the certificate
        status = self.check_certificate(ocsp_server, cert, issuer_cert)
        return status
