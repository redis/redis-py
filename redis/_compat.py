"""Internal module for Python 2 backwards compatibility."""
import sys


if sys.version_info[0] < 3:
    from urlparse import urlparse
    from itertools import imap, izip
    from string import letters as ascii_letters
    try:
        from cStringIO import StringIO as BytesIO
    except ImportError:
        from StringIO import StringIO as BytesIO

    iteritems = lambda x: x.iteritems()
    dictkeys = lambda x: x.keys()
    dictvalues = lambda x: x.values()
    nativestr = lambda x: \
        x if isinstance(x, str) else x.encode('utf-8', 'replace')
    u = lambda x: x.decode()
    b = lambda x: x
    next = lambda x: x.next()
    byte_to_chr = lambda x: x
    unichr = unichr
    xrange = xrange
    basestring = basestring
    unicode = unicode
    bytes = str
    long = long
else:
    from urllib.parse import urlparse
    from io import BytesIO
    from string import ascii_letters

    iteritems = lambda x: x.items()
    dictkeys = lambda x: list(x.keys())
    dictvalues = lambda x: list(x.values())
    byte_to_chr = lambda x: chr(x)
    nativestr = lambda x: \
        x if isinstance(x, str) else x.decode('utf-8', 'replace')
    u = lambda x: x
    b = lambda x: x.encode('iso-8859-1')
    next = next
    unichr = chr
    imap = map
    izip = zip
    xrange = range
    basestring = str
    unicode = str
    bytes = bytes
    long = int
