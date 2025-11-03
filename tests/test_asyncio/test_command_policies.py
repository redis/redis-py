import random

import pytest
from mock import patch

from redis import ResponseError
from redis._parsers.commands import CommandPolicies, RequestPolicy, ResponsePolicy
from redis.asyncio import RedisCluster
from redis.commands.policies import AsyncDynamicPolicyResolver, AsyncStaticPolicyResolver
from redis.commands.search.aggregation import AggregateRequest
from redis.commands.search.field import NumericField, TextField


@pytest.mark.asyncio
@pytest.mark.onlycluster
class TestBasePolicyResolver:
    async def test_resolve(self):
        zcount_policy = CommandPolicies(request_policy=RequestPolicy.DEFAULT_KEYED, response_policy=ResponsePolicy.DEFAULT_KEYED)
        rpoplpush_policy = CommandPolicies(request_policy=RequestPolicy.DEFAULT_KEYED, response_policy=ResponsePolicy.DEFAULT_KEYED)

        dynamic_resolver = AsyncDynamicPolicyResolver({
            'core': {
                'zcount': zcount_policy,
                'rpoplpush': rpoplpush_policy,
            }
        })
        assert await dynamic_resolver.resolve('zcount') == zcount_policy
        assert await dynamic_resolver.resolve('rpoplpush') == rpoplpush_policy

        with pytest.raises(ValueError, match="Wrong command or module name: foo.bar.baz"):
            await dynamic_resolver.resolve('foo.bar.baz')

        assert await dynamic_resolver.resolve('foo.bar') is None
        assert await dynamic_resolver.resolve('core.foo') is None

        # Test that policy fallback correctly
        static_resolver = AsyncStaticPolicyResolver()
        with_fallback_dynamic_resolver = dynamic_resolver.with_fallback(static_resolver)
        resolved_policies = await with_fallback_dynamic_resolver.resolve('ft.aggregate')

        assert resolved_policies.request_policy == RequestPolicy.DEFAULT_KEYLESS
        assert resolved_policies.response_policy == ResponsePolicy.DEFAULT_KEYLESS

        # Extended chain with one more resolver
        foo_bar_policy = CommandPolicies(request_policy=RequestPolicy.DEFAULT_KEYLESS, response_policy=ResponsePolicy.DEFAULT_KEYLESS)

        another_dynamic_resolver = AsyncDynamicPolicyResolver({
            'foo': {
                'bar': foo_bar_policy,
            }
        })
        with_fallback_static_resolver = static_resolver.with_fallback(another_dynamic_resolver)
        with_double_fallback_dynamic_resolver = dynamic_resolver.with_fallback(with_fallback_static_resolver)

        assert await with_double_fallback_dynamic_resolver.resolve('foo.bar') == foo_bar_policy

@pytest.mark.onlycluster
@pytest.mark.asyncio
class TestClusterWithPolicies:
    async def test_resolves_correctly_policies(self, r: RedisCluster, monkeypatch):
        # original nodes selection method
        determine_nodes = r._determine_nodes
        determined_nodes = []
        primary_nodes = r.get_primaries()
        calls = iter(list(range(len(primary_nodes))))

        async def wrapper(*args, request_policy: RequestPolicy, **kwargs):
            nonlocal determined_nodes
            determined_nodes = await determine_nodes(*args, request_policy=request_policy, **kwargs)
            return determined_nodes

        # Mock random.choice to always return a pre-defined sequence of nodes
        monkeypatch.setattr(random, "choice", lambda seq: seq[next(calls)])

        with patch.object(r, '_determine_nodes', side_effect=wrapper, autospec=True):
            # Routed to a random primary node
            await r.ft().create_index(
                [
                    NumericField("random_num"),
                    TextField("title"),
                    TextField("body"),
                    TextField("parent"),
                ]
            )
            assert determined_nodes[0] == primary_nodes[0]

            # Routed to another random primary node
            info = await r.ft().info()
            assert info['index_name'] == 'idx'
            assert determined_nodes[0] == primary_nodes[1]

            expected_node = await r.get_nodes_from_slot('FT.SUGLEN', *['foo'])
            await r.ft().suglen('foo')
            assert determined_nodes[0] == expected_node[0]

            # Indexing a document
            await r.hset(
                "search",
                mapping={
                    "title": "RediSearch",
                    "body": "Redisearch impements a search engine on top of redis",
                    "parent": "redis",
                    "random_num": 10,
                },
            )
            await r.hset(
                "ai",
                mapping={
                    "title": "RedisAI",
                    "body": "RedisAI executes Deep Learning/Machine Learning models and managing their data.",  # noqa
                    "parent": "redis",
                    "random_num": 3,
                },
            )
            await r.hset(
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
            res = await r.ft().aggregate(req)
            cursor = res.cursor

            # Ensure that aggregate node was cached.
            assert determined_nodes[0] == r._aggregate_nodes[0]

            await r.ft().aggregate(cursor)

            # Verify that FT.CURSOR dispatched to the same node.
            assert determined_nodes[0] == r._aggregate_nodes[0]

            # Error propagates to a user
            with pytest.raises(ResponseError, match="Cursor not found, id: 0"):
                await r.ft().aggregate(cursor)

            assert determined_nodes[0] == primary_nodes[2]

            # Core commands also randomly distributed across masters
            await r.randomkey()
            assert determined_nodes[0] == primary_nodes[0]