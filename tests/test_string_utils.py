import pytest

from redis.utils import (
    ensure_string,
    format_error_message,
    safe_str,
    str_if_bytes,
)


class TestStrIfBytes:
    def test_decodes_bytes(self):
        assert str_if_bytes(b"hello") == "hello"

    def test_passes_str_through(self):
        assert str_if_bytes("hello") == "hello"

    def test_replaces_invalid_utf8(self):
        # Undecodable bytes are replaced rather than raising.
        assert str_if_bytes(b"\xff") == "�"


class TestSafeStr:
    def test_stringifies_non_string_values(self):
        assert safe_str(123) == "123"
        assert safe_str(None) == "None"

    def test_decodes_bytes_first(self):
        assert safe_str(b"ab") == "ab"


class TestEnsureString:
    def test_decodes_bytes(self):
        assert ensure_string(b"key") == "key"

    def test_passes_str_through(self):
        assert ensure_string("key") == "key"

    def test_rejects_other_types(self):
        with pytest.raises(TypeError, match="string or bytes"):
            ensure_string(5)


class TestFormatErrorMessage:
    def test_no_args(self):
        assert format_error_message("host:1", Exception()) == (
            "Error connecting to host:1."
        )

    def test_single_arg(self):
        assert format_error_message("host:1", Exception("boom")) == (
            "Error boom connecting to host:1."
        )

    def test_two_args(self):
        assert format_error_message("host:1", Exception("code", "detail")) == (
            "Error code connecting to host:1. detail."
        )
