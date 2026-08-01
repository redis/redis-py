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
    # Python strings do not, so expand the shebang terminator escape.
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
    # When a real newline already ends the shebang line, do not rewrite
    # literal \n sequences that may appear in Lua source.
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
def test_normalize_function_lib_code_escapes_shebang_with_later_real_newlines():
    # Escaped shebang terminator must expand even when the Lua body already
    # has physical newlines (e.g. a multiline long string).
    code = (
        "#!lua name=mylib \\n redis.register_function('f', function()\n"
        "  return [[a\nb]]\nend)"
    )
    normalized = normalize_function_lib_code(code)
    shebang_line, rest = normalized.split("\n", 1)
    assert shebang_line.startswith("#!lua name=mylib")
    assert "\\n" not in shebang_line
    assert "[[a\nb]]" in rest


@pytest.mark.fixed_client
def test_normalize_function_lib_code_terminator_by_position_not_presence():
    # Prefer the earliest escape by position: shebang uses \n, body has \r\n.
    code = r"#!lua name=mylib \n return 'a\r\nb'"
    normalized = normalize_function_lib_code(code)
    assert normalized.count("\n") == 1
    body = normalized.split("\n", 1)[1]
    assert "\\r\\n" in body
    assert "return 'a\\r\\nb'" in normalized


@pytest.mark.fixed_client
def test_normalize_function_lib_code_bytes_roundtrip():
    code = b"\n#!lua name=mylib\nreturn 1\n"
    normalized = normalize_function_lib_code(code)
    assert isinstance(normalized, bytes)
    assert normalized.startswith(b"#!lua name=mylib\n")


@pytest.mark.fixed_client
def test_normalize_function_lib_code_bytes_non_utf8():
    # Non-UTF-8 body bytes must still get leading-whitespace / escape fixes.
    code = b"\n#!lua name=mylib \\n return '\xff'"
    normalized = normalize_function_lib_code(code)
    assert isinstance(normalized, bytes)
    assert not normalized.startswith(b"\n")
    shebang, body = normalized.split(b"\n", 1)
    assert shebang.startswith(b"#!lua name=mylib")
    assert b"\\n" not in shebang
    assert b"\xff" in body


@pytest.mark.fixed_client
def test_normalize_function_lib_code_handles_non_contiguous_memoryview():
    source = b"#!lua name=mylib\\nreturn 1"
    interleaved = bytearray(len(source) * 2)
    interleaved[::2] = source
    code = memoryview(interleaved)[::2]

    assert not code.c_contiguous
    assert normalize_function_lib_code(code) == b"#!lua name=mylib\nreturn 1"


@pytest.mark.fixed_client
def test_normalize_function_lib_code_preserves_body_line_endings():
    text = "#!lua name=mylib\r\nreturn [[a\n\rb\r\nc]]"
    expected = "#!lua name=mylib\nreturn [[a\n\rb\r\nc]]"

    assert normalize_function_lib_code(text) == expected
    assert normalize_function_lib_code(text.encode()) == expected.encode()


@pytest.mark.fixed_client
def test_normalize_function_lib_code_expands_terminator_before_lfcr():
    text = r"#!lua name=mylib \n" + "\rreturn 1"
    expected = "#!lua name=mylib \nreturn 1"

    assert normalize_function_lib_code(text) == expected
    assert normalize_function_lib_code(text.encode()) == expected.encode()


@pytest.mark.fixed_client
def test_normalize_function_lib_code_preserves_body_cr_after_escaped_crlf():
    text = r"#!lua name=mylib \\r\\n" + "\rreturn 1"
    expected = "#!lua name=mylib \n\rreturn 1"

    assert normalize_function_lib_code(text) == expected
    assert normalize_function_lib_code(text.encode()) == expected.encode()


@pytest.mark.fixed_client
def test_normalize_function_lib_code_preserves_trailing_whitespace():
    text = "#!lua name=mylib\nreturn 1\n \t"
    binary = text.encode()

    assert normalize_function_lib_code(text) == text
    assert normalize_function_lib_code(binary) == binary


@pytest.mark.fixed_client
def test_normalize_function_lib_code_sliced_memoryview_no_copy_when_unchanged():
    source = b"pad#!lua name=mylib\nreturn 1\r\ntail"
    code = memoryview(source)[3:-4]

    assert normalize_function_lib_code(code) is code


@pytest.mark.fixed_client
@pytest.mark.parametrize(
    "code",
    [
        b"#!lua name=mylib\nreturn 1",
        bytearray(b"#!lua name=mylib\nreturn 1"),
        memoryview(b"#!lua name=mylib\nreturn 1"),
    ],
)
def test_normalize_function_lib_code_binary_no_copy_when_unchanged(code):
    assert normalize_function_lib_code(code) is code

