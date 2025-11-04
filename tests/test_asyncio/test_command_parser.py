import pytest

from redis._parsers import AsyncCommandsParser
from redis._parsers.commands import RequestPolicy, ResponsePolicy
from tests.conftest import skip_if_server_version_lt


@pytest.mark.onlycluster
@skip_if_server_version_lt("8.0.0")
class TestAsyncCommandParser:
    @pytest.mark.asyncio
    async def test_get_command_policies(self, r):
        commands_parser = AsyncCommandsParser()
        await commands_parser.initialize(node=r.get_default_node())
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

        actual_policies = await commands_parser.get_command_policies()
        assert len(actual_policies) > 0

        for module_name, commands in expected_command_policies.items():
            for command, command_policies in commands.items():
                assert command in actual_policies[module_name]
                assert command_policies == [
                    command,
                    actual_policies[module_name][command].request_policy,
                    actual_policies[module_name][command].response_policy,
                ]
