import pytest

def safe_str_encode(val, encoding="utf-8", errors="strict"):
    """Encodes string to bytes if not already bytes."""
    if val is None:
        return None
    if isinstance(val, bytes):
        return val
    if isinstance(val, (int, float, bool)):
        return str(val).encode(encoding, errors)
    return str(val).encode(encoding, errors)

def test_safe_str_encode_bytes_passthrough():
    assert safe_str_encode(b"redis_key") == b"redis_key"

def test_safe_str_encode_str_to_bytes():
    assert safe_str_encode("user:1001:session") == b"user:1001:session"

def test_safe_str_encode_numeric_types():
    assert safe_str_encode(42) == b"42"
    assert safe_str_encode(3.14) == b"3.14"
    assert safe_str_encode(True) == b"True"

def test_safe_str_encode_none():
    assert safe_str_encode(None) is None
