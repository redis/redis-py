try:
    import hiredis  # noqa
    HIREDIS_AVAILABLE = True
except ImportError:
    HIREDIS_AVAILABLE = False
