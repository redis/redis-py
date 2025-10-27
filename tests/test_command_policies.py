import random
from unittest.mock import Mock, patch

import pytest

from redis import ResponseError

from redis._parsers import CommandsParser
from redis._parsers.commands import CommandPolicies, RequestPolicy, ResponsePolicy
from redis.commands.policies import DynamicPolicyResolver, StaticPolicyResolver
from redis.commands.search.aggregation import AggregateRequest
from redis.commands.search.field import TextField, NumericField

@pytest.mark.onlycluster
class TestBasePolicyResolver:
    def test_resolve(self):
        mock_command_parser = Mock(spec=CommandsParser)
        zcount_policy = CommandPolicies(request_policy=RequestPolicy.DEFAULT_KEYED, response_policy=ResponsePolicy.DEFAULT_KEYED)
        rpoplpush_policy = CommandPolicies(request_policy=RequestPolicy.DEFAULT_KEYED, response_policy=ResponsePolicy.DEFAULT_KEYED)

        mock_command_parser.get_command_policies.return_value = {
            'core': {
                'zcount': zcount_policy,
                'rpoplpush': rpoplpush_policy,
            }
        }

        dynamic_resolver = DynamicPolicyResolver(mock_command_parser)
        assert dynamic_resolver.resolve('zcount') == zcount_policy
        assert dynamic_resolver.resolve('rpoplpush') == rpoplpush_policy

        with pytest.raises(ValueError, match="Wrong command or module name: foo.bar.baz"):
            dynamic_resolver.resolve('foo.bar.baz')

        assert dynamic_resolver.resolve('foo.bar') is None
        assert dynamic_resolver.resolve('core.foo') is None

        # Test that policy fallback correctly
        static_resolver = StaticPolicyResolver()
        with_fallback_dynamic_resolver = dynamic_resolver.with_fallback(static_resolver)

        assert with_fallback_dynamic_resolver.resolve('ft.aggregate').request_policy == RequestPolicy.DEFAULT_KEYLESS
        assert with_fallback_dynamic_resolver.resolve('ft.aggregate').response_policy == ResponsePolicy.DEFAULT_KEYLESS

        # Extended chain with one more resolver
        mock_command_parser = Mock(spec=CommandsParser)
        foo_bar_policy = CommandPolicies(request_policy=RequestPolicy.DEFAULT_KEYLESS, response_policy=ResponsePolicy.DEFAULT_KEYLESS)

        mock_command_parser.get_command_policies.return_value = {
            'foo': {
                'bar': foo_bar_policy,
            }
        }
        another_dynamic_resolver = DynamicPolicyResolver(mock_command_parser)
        with_fallback_static_resolver = static_resolver.with_fallback(another_dynamic_resolver)
        with_double_fallback_dynamic_resolver = dynamic_resolver.with_fallback(with_fallback_static_resolver)

        assert with_double_fallback_dynamic_resolver.resolve('foo.bar') == foo_bar_policy

@pytest.mark.onlycluster
class TestClusterWithPolicies:
    def test_resolves_correctly_policies(self, r, monkeypatch):
        # original nodes selection method
        determine_nodes = r._determine_nodes
        determined_nodes = []
        primary_nodes = r.get_primaries()
        calls = iter(list(range(len(primary_nodes))))

        def wrapper(*args, request_policy: RequestPolicy, **kwargs):
            nonlocal determined_nodes
            determined_nodes = determine_nodes(*args, request_policy=request_policy, **kwargs)
            return determined_nodes

        # Mock random.choice to always return a pre-defined sequence of nodes
        monkeypatch.setattr(random, "choice", lambda seq: seq[next(calls)])

        with patch.object(r, '_determine_nodes', side_effect=wrapper, autospec=True):
            # Routed to a random primary node
            r.ft().create_index(
                (
                    NumericField("random_num"),
                    TextField("title"),
                    TextField("body"),
                    TextField("parent"),
                )
            )
            assert determined_nodes[0] == primary_nodes[0]

            # Routed to another random primary node
            info = r.ft().info()
            assert info['index_name'] == 'idx'
            assert determined_nodes[0] == primary_nodes[1]

            expected_node = r.get_node_from_slot('ft.suglen', *['FT.SUGLEN', 'foo'])
            r.ft().suglen('foo')
            assert determined_nodes[0] == expected_node[0]

            # Indexing a document
            r.hset(
                "search",
                mapping={
                    "title": "RediSearch",
                    "body": "Redisearch impements a search engine on top of redis",
                    "parent": "redis",
                    "random_num": 10,
                },
            )
            r.hset(
                "ai",
                mapping={
                    "title": "RedisAI",
                    "body": "RedisAI executes Deep Learning/Machine Learning models and managing their data.",  # noqa
                    "parent": "redis",
                    "random_num": 3,
                },
            )
            r.hset(
                "json",
                mapping={
                    "title": "RedisJson",
                    "body": "RedisJSON implements ECMA-404 The JSON Data Interchange Standard as a native data type.",  # noqa
                    "parent": "redis",
                    "random_num": 8,
                },
            )

            req = AggregateRequest("redis").group_by(
                "@parent"
            ).cursor(1)
            cursor = r.ft().aggregate(req).cursor

            # Ensure that aggregate node was cached.
            assert determined_nodes[0] == r._aggregate_nodes[0]

            r.ft().aggregate(cursor)

            # Verify that FT.CURSOR dispatched to the same node.
            assert determined_nodes[0] == r._aggregate_nodes[0]

            # Error propagates to a user
            with pytest.raises(ResponseError, match="Cursor not found, id: 0"):
                r.ft().aggregate(cursor)