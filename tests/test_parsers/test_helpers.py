import pytest

from redis._parsers.helpers import (
    pairs_to_dict,
    pairs_to_dict_typed,
    parse_acl_log,
    parse_acl_log_resp3_to_resp2_legacy,
    parse_client_list,
    parse_command,
    parse_info,
    parse_sentinel_masters_resp3,
    zpop_score_pairs,
)


@pytest.mark.fixed_client
def test_parse_info():
    info_output = """
# Modules
module:name=search,ver=999999,api=1,filters=0,usedby=[],using=[ReJSON],options=[handle-io-errors]

# search_fields_statistics
search_fields_text:Text=3
search_fields_tag:Tag=2,Sortable=1

# search_version
search_version:99.99.99
search_redis_version:7.2.2 - oss

# search_runtime_configurations
search_query_timeout_ms:500
    """
    info = parse_info(info_output)

    assert isinstance(info["modules"], list)
    assert isinstance(info["modules"][0], dict)
    assert info["modules"][0]["name"] == "search"

    assert isinstance(info["search_fields_text"], dict)
    assert info["search_fields_text"]["Text"] == 3

    assert isinstance(info["search_fields_tag"], dict)
    assert info["search_fields_tag"]["Tag"] == 2
    assert info["search_fields_tag"]["Sortable"] == 1

    assert info["search_version"] == "99.99.99"
    assert info["search_redis_version"] == "7.2.2 - oss"
    assert info["search_query_timeout_ms"] == 500


@pytest.mark.fixed_client
def test_parse_info_list():
    info_output = """
list_one:a,
list_two:a b,,c,10,1.1
    """
    info = parse_info(info_output)

    assert isinstance(info["list_one"], list)
    assert info["list_one"] == ["a"]

    assert isinstance(info["list_two"], list)
    assert info["list_two"] == ["a b", "c", 10, 1.1]


@pytest.mark.fixed_client
def test_parse_info_list_dict_mixed():
    info_output = """
list_one:a,b=1
list_two:a b=foo,,c,d=bar,e,
    """
    info = parse_info(info_output)

    assert isinstance(info["list_one"], dict)
    assert info["list_one"] == {"a": True, "b": 1}

    assert isinstance(info["list_two"], dict)
    assert info["list_two"] == {"a b": "foo", "c": True, "d": "bar", "e": True}


@pytest.mark.fixed_client
def test_parse_client_list():
    response = "id=7 addr=/tmp/redis sock/redis.sock:0 fd=9 name=test=_complex_[name] age=-1 idle=0 cmd=client|list user=default lib-name=go-redis(,go1.24.4) lib-ver="
    expected = [
        {
            "id": "7",
            "addr": "/tmp/redis sock/redis.sock:0",
            "fd": "9",
            "name": "test=_complex_[name]",
            "age": "-1",
            "idle": "0",
            "cmd": "client|list",
            "user": "default",
            "lib-name": "go-redis(,go1.24.4)",
            "lib-ver": "",
        }
    ]
    clients = parse_client_list(response)
    assert clients == expected


@pytest.mark.fixed_client
def test_parse_command_preserves_acl_categories():
    response = [
        [
            b"get",
            2,
            [b"readonly", b"fast"],
            1,
            1,
            1,
            [b"@read", b"@string", b"@fast"],
            [b"request_policy:all_shards"],
            [],
            [],
        ]
    ]

    command = parse_command(response)["get"]

    assert command["flags"] == ["readonly", "fast"]
    assert command["acl_categories"] == ["@read", "@string", "@fast"]


@pytest.mark.fixed_client
def test_zpop_score_pairs_resp2_always_pairs_scores():
    response = [b"member1", b"1", b"member2", b"2.5"]

    assert zpop_score_pairs(response) == [(b"member1", 1.0), (b"member2", 2.5)]


@pytest.mark.fixed_client
def test_parse_sentinel_masters_resp3_returns_master_dict():
    response = [
        {
            b"name": b"redis-py-test",
            b"ip": b"127.0.0.1",
            b"port": b"6379",
            b"flags": b"master",
            b"num-other-sentinels": b"1",
        }
    ]

    masters = parse_sentinel_masters_resp3(response)

    assert set(masters) == {"redis-py-test"}
    assert masters["redis-py-test"]["flags"] == {"master"}
    assert masters["redis-py-test"]["is_master"] is True
    assert masters["redis-py-test"]["is_sdown"] is False


@pytest.mark.fixed_client
def test_parse_acl_log_resp3_legacy_decodes_string_values():
    # On a RESP3 connection with the default legacy_responses=True, each ACL
    # LOG entry arrives as a map. The scalar string fields (reason, context,
    # object, username) are bulk strings and must be decoded to ``str`` so the
    # result matches what parse_acl_log() produces from a RESP2 connection.
    client_info = b"id=3 addr=127.0.0.1:52654 name= age=0 user=someuser"
    resp3_entry = {
        b"count": 1,
        b"reason": b"auth",
        b"context": b"toplevel",
        b"object": b"AUTH",
        b"username": b"someuser",
        b"age-seconds": b"8.038",
        b"client-info": client_info,
        b"entry-id": 0,
        b"timestamp-created": 1700000000000,
        b"timestamp-last-updated": 1700000000000,
    }

    parsed = parse_acl_log_resp3_to_resp2_legacy([resp3_entry])[0]

    assert parsed["reason"] == "auth"
    assert parsed["context"] == "toplevel"
    assert parsed["object"] == "AUTH"
    assert parsed["username"] == "someuser"

    # Must match the RESP2-wire legacy shape exactly (the default-config promise).
    resp2_entry = [
        b"count",
        1,
        b"reason",
        b"auth",
        b"context",
        b"toplevel",
        b"object",
        b"AUTH",
        b"username",
        b"someuser",
        b"age-seconds",
        b"8.038",
        b"client-info",
        client_info,
        b"entry-id",
        0,
        b"timestamp-created",
        1700000000000,
        b"timestamp-last-updated",
        1700000000000,
    ]
    assert parse_acl_log([resp2_entry]) == [parsed]


# pairs_to_dict / pairs_to_dict_typed turn a flat [k1, v1, k2, v2] reply into a
# dict and back the response callbacks. These cover their behavior (decoding
# flags, type coercion and its fallback) without a live server; they were
# previously only exercised indirectly through integration tests.


@pytest.mark.fixed_client
def test_pairs_to_dict_none_returns_empty_dict():
    assert pairs_to_dict(None) == {}


@pytest.mark.fixed_client
def test_pairs_to_dict_empty_list_returns_empty_dict():
    assert pairs_to_dict([]) == {}


@pytest.mark.fixed_client
def test_pairs_to_dict_basic_pairs_preserve_types():
    # Without any decode flag, keys and values are passed through as-is.
    assert pairs_to_dict([b"a", b"1", b"b", b"2"]) == {b"a": b"1", b"b": b"2"}


@pytest.mark.fixed_client
def test_pairs_to_dict_decode_keys_only():
    # Keys are decoded to str; values are left untouched.
    assert pairs_to_dict([b"a", b"1"], decode_keys=True) == {"a": b"1"}


@pytest.mark.fixed_client
def test_pairs_to_dict_decode_string_values_only():
    # Values are decoded to str; keys are left untouched.
    assert pairs_to_dict([b"a", b"1"], decode_string_values=True) == {b"a": "1"}


@pytest.mark.fixed_client
def test_pairs_to_dict_decode_keys_and_values():
    result = pairs_to_dict(
        [b"a", b"1", b"b", b"2"], decode_keys=True, decode_string_values=True
    )
    assert result == {"a": "1", "b": "2"}


@pytest.mark.fixed_client
def test_pairs_to_dict_non_bytes_values_pass_through_when_decoding():
    # str_if_bytes only decodes bytes; ints and strs are returned unchanged.
    assert pairs_to_dict([b"n", 5], decode_string_values=True) == {b"n": 5}


@pytest.mark.fixed_client
def test_pairs_to_dict_typed_empty_list_returns_empty_dict():
    assert pairs_to_dict_typed([], {}) == {}


@pytest.mark.fixed_client
def test_pairs_to_dict_typed_coerces_value_for_known_key():
    result = pairs_to_dict_typed(["count", "5"], {"count": int})
    assert result == {"count": 5}
    assert isinstance(result["count"], int)


@pytest.mark.fixed_client
def test_pairs_to_dict_typed_uncoercible_value_falls_back_to_original():
    # int("abc") raises, so the raw value is kept instead of blowing up.
    assert pairs_to_dict_typed(["count", "abc"], {"count": int}) == {"count": "abc"}


@pytest.mark.fixed_client
def test_pairs_to_dict_typed_unknown_key_is_left_unchanged():
    assert pairs_to_dict_typed(["name", "redis"], {"count": int}) == {"name": "redis"}


@pytest.mark.fixed_client
def test_pairs_to_dict_typed_mixed_typed_and_untyped_keys():
    result = pairs_to_dict_typed(
        ["count", "5", "ratio", "1.5", "name", "redis"],
        {"count": int, "ratio": float},
    )
    assert result == {"count": 5, "ratio": 1.5, "name": "redis"}
