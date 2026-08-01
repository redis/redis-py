import string

import pytest

from redis.commands.helpers import (
    delist,
    list_or_args,
    nativestr,
    normalize_function_lib_code,
    parse_to_list,
    random_string,
)


@pytest.mark.fixed_client
def test_list_or_args():
    k = ["hello, world"]
    a = ["some", "argument", "list"]
    assert list_or_args(k, a) == k + a

    for i in ["banana", b"banana", bytearray(b"banana"), memoryview(b"banana")]:
        assert list_or_args(i, a) == [i] + a


@pytest.mark.fixed_client
def test_parse_to_list():
    assert parse_to_list(None) == []
    r = ["hello", b"my name", "45", "555.55", "is simon!", None]
    assert parse_to_list(r) == ["hello", "my name", 45, 555.55, "is simon!", None]


@pytest.mark.fixed_client
def test_nativestr():
    assert nativestr("teststr") == "teststr"
    assert nativestr(b"teststr") == "teststr"
    assert nativestr("null") is None


@pytest.mark.fixed_client
def test_delist():
    assert delist(None) is None
    assert delist([b"hello", "world", b"banana"]) == ["hello", "world", "banana"]


@pytest.mark.fixed_client
def test_random_string():
    assert len(random_string()) == 10
    assert len(random_string(15)) == 15
    for a in random_string():
        assert a in string.ascii_lowercase


@pytest.mark.fixed_client
def test_normalize_function_lib_code_strips_leading_whitespace():
    # Issue #3307: triple-quoted multi-line payloads often start with a newline
    # before the shebang, which Redis rejects as "Missing library metadata".
    code = """
#!lua name=mylib
redis.register_function('myfunc', function(keys, args) return args[1] end)
"""
    normalized = normalize_function_lib_code(code)
    assert normalized.startswith("#!lua name=mylib\n")
    assert "redis.register_function" in normalized


@pytest.mark.fixed_client
def test_normalize_function_lib_code_expands_redis_cli_escapes():
    # redis-cli unescapes "\n" inside double quotes; raw / double-escaped
    # Python strings do not, so expand the shebang terminator when no real
    # newline is present.
    code = r"#!lua name=mylib \n redis.register_function('myfunc', function(keys, args) return args[1] end)"
    normalized = normalize_function_lib_code(code)
    assert "\n" in normalized
    assert "\\n" not in normalized
    assert normalized.startswith("#!lua name=mylib")
    assert normalized.split("\n", 1)[1].lstrip().startswith(
        "redis.register_function"
    )


@pytest.mark.fixed_client
def test_normalize_function_lib_code_normalizes_crlf():
    code = "#!lua name=mylib\r\nredis.register_function('myfunc', function() end)"
    assert normalize_function_lib_code(code) == (
        "#!lua name=mylib\nredis.register_function('myfunc', function() end)"
    )


@pytest.mark.fixed_client
def test_normalize_function_lib_code_preserves_multiline_backslash_n():
    # When real newlines already exist, do not rewrite literal \n sequences
    # that may appear in Lua source.
    code = "#!lua name=mylib\nreturn 'a\\nb'"
    assert normalize_function_lib_code(code) == code


@pytest.mark.fixed_client
def test_normalize_function_lib_code_expands_only_shebang_terminator():
    # Single-line redis-cli-style payloads may also contain intentional
    # backslash-n in Lua source; only the first escape (shebang separator)
    # should become a real newline.
    code = r"#!lua name=mylib \n redis.register_function('f', function() return 'a\nb' end)"
    normalized = normalize_function_lib_code(code)
    assert normalized.startswith("#!lua name=mylib\n") or normalized.startswith(
        "#!lua name=mylib \n"
    )
    # Exactly one real newline (the shebang terminator).
    assert normalized.count("\n") == 1
    # Body Lua escape preserved as two characters.
    assert "return 'a\\nb'" in normalized
    assert "\\n" in normalized.split("\n", 1)[1]


@pytest.mark.fixed_client
def test_normalize_function_lib_code_expands_first_crlf_escape():
    code = r"#!lua name=mylib \r\n return 'a\nb'"
    normalized = normalize_function_lib_code(code)
    assert normalized.count("\n") == 1
    assert "return 'a\\nb'" in normalized


@pytest.mark.fixed_client
def test_normalize_function_lib_code_bytes_roundtrip():
    code = b"\n#!lua name=mylib\nreturn 1\n"
    normalized = normalize_function_lib_code(code)
    assert isinstance(normalized, bytes)
    assert normalized.startswith(b"#!lua name=mylib\n")
