from redis._parsers.helpers import parse_info


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
