import sys

__all__ = ['basestring', 'bytes', 'imap', 'izip', 'long', 'unicode', 'unichr', 'xrange']

MAJOR_VERSION = sys.version_info.major

if MAJOR_VERSION >= 3:
    basestring = str
    bytes = bytes
    imap = map
    izip = zip
    long = int
    unichr = chr
    unicode = str
    xrange = range
else:
    from itertools import imap, izip

    basestring = basestring
    try:
        bytes = bytes
    except NameError:
        bytes = str
    long = long
    unichr = unichr
    unicode = unicode
    xrange = xrange
