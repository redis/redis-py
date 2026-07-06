from redis.utils import decode_field_value


def test_non_bytes_value_returned_unchanged():
    assert decode_field_value("already-str") == "already-str"


def test_bytes_default_decoded_to_str():
    assert decode_field_value(b"hi") == "hi"


def test_per_field_encoding_is_applied():
    result = decode_field_value(b"caf\xe9", key="k", field_encodings={"k": "latin-1"})
    assert result == "caf\xe9"


def test_none_encoding_keeps_raw_bytes():
    assert decode_field_value(b"raw", key="k", field_encodings={"k": None}) == b"raw"


def test_key_absent_from_encodings_uses_default():
    assert decode_field_value(b"hi", key="x", field_encodings={"k": "utf-8"}) == "hi"


def test_undecodable_bytes_use_replacement_character():
    result = decode_field_value(b"\xff", key="k", field_encodings={"k": "utf-8"})
    assert result == "�"
