from pprint import pprint

import pytest
from redis._parsers import CommandsParser
from redis._parsers.commands import RequestPolicy, ResponsePolicy

from .conftest import (
    assert_resp_response,
    skip_if_redis_enterprise,
    skip_if_server_version_lt,
)


class TestCommandsParser:
    def test_init_commands(self, r):
        commands_parser = CommandsParser(r)
        assert commands_parser.commands is not None
        assert "get" in commands_parser.commands

    def test_get_keys_predetermined_key_location(self, r):
        commands_parser = CommandsParser(r)
        args1 = ["GET", "foo"]
        args2 = ["OBJECT", "encoding", "foo"]
        args3 = ["MGET", "foo", "bar", "foobar"]
        assert commands_parser.get_keys(r, *args1) == ["foo"]
        assert commands_parser.get_keys(r, *args2) == ["foo"]
        assert commands_parser.get_keys(r, *args3) == ["foo", "bar", "foobar"]

    @pytest.mark.filterwarnings("ignore:ResponseError")
    @skip_if_redis_enterprise()
    def test_get_moveable_keys(self, r):
        commands_parser = CommandsParser(r)
        args1 = [
            "EVAL",
            "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}",
            2,
            "key1",
            "key2",
            "first",
            "second",
        ]
        args2 = ["XREAD", "COUNT", 2, b"STREAMS", "mystream", "writers", 0, 0]
        args3 = ["ZUNIONSTORE", "out", 2, "zset1", "zset2", "WEIGHTS", 2, 3]
        args4 = ["GEORADIUS", "Sicily", 15, 37, 200, "km", "WITHCOORD", b"STORE", "out"]
        args5 = ["MEMORY USAGE", "foo"]
        args6 = [
            "MIGRATE",
            "192.168.1.34",
            6379,
            "",
            0,
            5000,
            b"KEYS",
            "key1",
            "key2",
            "key3",
        ]
        args7 = ["MIGRATE", "192.168.1.34", 6379, "key1", 0, 5000]

        assert_resp_response(
            r,
            sorted(commands_parser.get_keys(r, *args1)),
            ["key1", "key2"],
            [b"key1", b"key2"],
        )
        assert_resp_response(
            r,
            sorted(commands_parser.get_keys(r, *args2)),
            ["mystream", "writers"],
            [b"mystream", b"writers"],
        )
        assert_resp_response(
            r,
            sorted(commands_parser.get_keys(r, *args3)),
            ["out", "zset1", "zset2"],
            [b"out", b"zset1", b"zset2"],
        )
        assert_resp_response(
            r,
            sorted(commands_parser.get_keys(r, *args4)),
            ["Sicily", "out"],
            [b"Sicily", b"out"],
        )
        assert sorted(commands_parser.get_keys(r, *args5)) in [["foo"], [b"foo"]]
        assert_resp_response(
            r,
            sorted(commands_parser.get_keys(r, *args6)),
            ["key1", "key2", "key3"],
            [b"key1", b"key2", b"key3"],
        )
        assert_resp_response(
            r, sorted(commands_parser.get_keys(r, *args7)), ["key1"], [b"key1"]
        )

    # A bug in redis<7.0 causes this to fail: https://github.com/redis/redis/issues/9493
    @skip_if_server_version_lt("7.0.0")
    def test_get_eval_keys_with_0_keys(self, r):
        commands_parser = CommandsParser(r)
        args = ["EVAL", "return {ARGV[1],ARGV[2]}", 0, "key1", "key2"]
        assert commands_parser.get_keys(r, *args) == []

    def test_get_pubsub_keys(self, r):
        commands_parser = CommandsParser(r)
        args1 = ["PUBLISH", "foo", "bar"]
        args2 = ["PUBSUB NUMSUB", "foo1", "foo2", "foo3"]
        args3 = ["PUBSUB channels", "*"]
        args4 = ["SUBSCRIBE", "foo1", "foo2", "foo3"]
        assert commands_parser.get_keys(r, *args1) == ["foo"]
        assert commands_parser.get_keys(r, *args2) == ["foo1", "foo2", "foo3"]
        assert commands_parser.get_keys(r, *args3) == ["*"]
        assert commands_parser.get_keys(r, *args4) == ["foo1", "foo2", "foo3"]

    @skip_if_server_version_lt("7.0.0")
    @pytest.mark.onlycluster
    def test_get_command_policies(self, r):
        commands_parser = CommandsParser(r)
        expected_command_policies = {
            "core": {
                "keys": [
                    "keys",
                    RequestPolicy.ALL_SHARDS,
                    ResponsePolicy.DEFAULT_KEYLESS,
                ],
                "acl setuser": [
                    "acl setuser",
                    RequestPolicy.ALL_NODES,
                    ResponsePolicy.ALL_SUCCEEDED,
                ],
                "exists": ["exists", RequestPolicy.MULTI_SHARD, ResponsePolicy.AGG_SUM],
                "config resetstat": [
                    "config resetstat",
                    RequestPolicy.ALL_NODES,
                    ResponsePolicy.ALL_SUCCEEDED,
                ],
                "slowlog len": [
                    "slowlog len",
                    RequestPolicy.ALL_NODES,
                    ResponsePolicy.AGG_SUM,
                ],
                "scan": ["scan", RequestPolicy.SPECIAL, ResponsePolicy.SPECIAL],
                "latency history": [
                    "latency history",
                    RequestPolicy.ALL_NODES,
                    ResponsePolicy.SPECIAL,
                ],
                "memory doctor": [
                    "memory doctor",
                    RequestPolicy.ALL_SHARDS,
                    ResponsePolicy.SPECIAL,
                ],
                "randomkey": [
                    "randomkey",
                    RequestPolicy.ALL_SHARDS,
                    ResponsePolicy.SPECIAL,
                ],
                "mget": [
                    "mget",
                    RequestPolicy.MULTI_SHARD,
                    ResponsePolicy.DEFAULT_KEYED,
                ],
                "function restore": [
                    "function restore",
                    RequestPolicy.ALL_SHARDS,
                    ResponsePolicy.ALL_SUCCEEDED,
                ],
            },
            "json": {
                "debug": [
                    "debug",
                    RequestPolicy.DEFAULT_KEYED,
                    ResponsePolicy.DEFAULT_KEYED,
                ],
                "get": [
                    "get",
                    RequestPolicy.DEFAULT_KEYED,
                    ResponsePolicy.DEFAULT_KEYED,
                ],
            },
            "ft": {
                "search": [
                    "search",
                    RequestPolicy.DEFAULT_KEYLESS,
                    ResponsePolicy.DEFAULT_KEYLESS,
                ],
                "create": [
                    "create",
                    RequestPolicy.DEFAULT_KEYLESS,
                    ResponsePolicy.DEFAULT_KEYLESS,
                ],
            },
            "bf": {
                "add": [
                    "add",
                    RequestPolicy.DEFAULT_KEYED,
                    ResponsePolicy.DEFAULT_KEYED,
                ],
                "madd": [
                    "madd",
                    RequestPolicy.DEFAULT_KEYED,
                    ResponsePolicy.DEFAULT_KEYED,
                ],
            },
            "cf": {
                "add": [
                    "add",
                    RequestPolicy.DEFAULT_KEYED,
                    ResponsePolicy.DEFAULT_KEYED,
                ],
                "mexists": [
                    "mexists",
                    RequestPolicy.DEFAULT_KEYED,
                    ResponsePolicy.DEFAULT_KEYED,
                ],
            },
            "tdigest": {
                "add": [
                    "add",
                    RequestPolicy.DEFAULT_KEYED,
                    ResponsePolicy.DEFAULT_KEYED,
                ],
                "min": [
                    "min",
                    RequestPolicy.DEFAULT_KEYED,
                    ResponsePolicy.DEFAULT_KEYED,
                ],
            },
            "ts": {
                "create": [
                    "create",
                    RequestPolicy.DEFAULT_KEYED,
                    ResponsePolicy.DEFAULT_KEYED,
                ],
                "info": [
                    "info",
                    RequestPolicy.DEFAULT_KEYED,
                    ResponsePolicy.DEFAULT_KEYED,
                ],
            },
            "topk": {
                "list": [
                    "list",
                    RequestPolicy.DEFAULT_KEYED,
                    ResponsePolicy.DEFAULT_KEYED,
                ],
                "query": [
                    "query",
                    RequestPolicy.DEFAULT_KEYED,
                    ResponsePolicy.DEFAULT_KEYED,
                ],
            },
        }

        actual_policies = commands_parser.get_command_policies()
        assert len(actual_policies) > 0

        for module_name, commands in expected_command_policies.items():
            for command, command_policies in commands.items():
                assert command in actual_policies[module_name]
                assert command_policies == [
                    command,
                    actual_policies[module_name][command].request_policy,
                    actual_policies[module_name][command].response_policy,
                ]
