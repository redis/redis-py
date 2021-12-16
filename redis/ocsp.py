from redis.exceptions import ConnectionError
import ssl

from cryptography import x509
from cryptography import hazmat

def get_ascii_certificate(sock):
    der = sock.getpeercert(True)
    pem = ssl.DER_cert_to_PEM_cert(der)
    cert = x509.load_pem_409_certificate(pem.encode('ascii'), 
                                         hazmat.backends.default_backend())
    
    aia = cert.extensions.get_extension_for_oid(509.oi.ExtensionOID.AUTHORITY_INFORMATION_ACCESS).value
    
    # fetch certificate issuers
    issuers = [i for i in aia if i.access_method == x509.oid.AuthorityInformationAccessOID.CA_ISSUERS]
    try:
        issuer = issuers[0].access_location.value
    except IndexError:
        raise ConnectionError(f'certificate contains no issuers.')
    
    # now, the series of ocsp server entries
    ocsps = [i for i in aia if i.access_method == x509.oid.AuthorityInformationAccessOID.OCSP]
    try:
        ocsp = ocsps[0].access_location.value
    except IndexError:
        raise ConnectionError(f'certificate contains no ocsp server')
    
    return cert, issuer, ocsp

def is_valid_ocsp(hostname, port):
    pass