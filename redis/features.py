import warnings
from distutils.version import StrictVersion


# This module sets several flags based on the running environment:
#
#   HIREDIS_AVAILABLE: boolean indicating whether a suitable hiredis-py
#                      version is installed.
#   SSL_AVAILABLE: boolean indicating whether the ssl module was installed
#                  when Python was built.

# attempt to import hiredis. if it is importable and new enough, flag that
# hiredis support is enabled
HIREDIS_AVAILABLE = False
try:
    import hiredis
    HIREDIS_VERSION = StrictVersion(hiredis.__version__)
    if HIREDIS_VERSION < StrictVersion('1.1.0'):
        msg = ('redis-py requires hiredis >= 1.1.0 to enable hiredis support. '
               'You are running hiredis version %s. Please upgrade hiredis '
               'to enable hiredis optimizations.') % HIREDIS_VERSION
        warnings.warn(msg)
    else:
        HIREDIS_AVAILABLE = True

except ImportError:
    pass


# attempt to import ssl. if it is importable, flag that ssl support is enabled
try:
    import ssl  # noqa
    SSL_AVAILABLE = True
except ImportError:
    SSL_AVAILABLE = False
