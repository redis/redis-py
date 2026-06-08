"""
Basic integration test for the (async) ``MultiDBClient`` running on top of two
real Redis deployments.

The same test runs in two configurations against the local docker-compose
infrastructure:

* ``double-cluster``    -> two independent ``RedisCluster`` deployments
  (``cluster`` on 16379 and ``cluster2`` on 16385);
* ``double-standalone`` -> two independent ``Redis`` deployments
  (``redis`` on 6379 and ``redis-stack`` on 6479).

Nothing is mocked here - every multi-db component as well as the underlying
async ``Redis`` / ``RedisCluster`` clients are real and talk to real servers.

Run with: ``invoke multidb-integration-tests`` (after ``invoke devenv``).
"""

import pytest

from redis.asyncio import Redis, RedisCluster
from redis.asyncio.multidb.client import MultiDBClient
from redis.asyncio.multidb.config import DatabaseConfig, MultiDbConfig

from tests.test_multidb._integration_endpoints import get_endpoints


def _build_config(client_class, urls):
    db_configs = [
        DatabaseConfig(
            weight=1.0,
            from_url=urls[0],
            client_kwargs={"decode_responses": True},
        ),
        DatabaseConfig(
            weight=0.9,
            from_url=urls[1],
            client_kwargs={"decode_responses": True},
        ),
    ]
    return MultiDbConfig(
        client_class=client_class,
        databases_config=db_configs,
        # Keep the initial health check quick for the test.
        health_check_probes=3,
        health_check_delay=0.1,
        health_check_interval=5,
    )


@pytest.mark.multidb_integration
@pytest.mark.no_mock_connections
@pytest.mark.parametrize(
    "topology,client_class",
    [
        ("cluster", RedisCluster),
        ("standalone", Redis),
    ],
    ids=["double-cluster", "double-standalone"],
)
class TestMultiDbClientIntegration:
    @pytest.mark.asyncio
    async def test_ping_against_two_databases(self, topology, client_class):
        urls = get_endpoints(topology)

        async with MultiDBClient(_build_config(client_class, urls)) as client:
            # PING goes through the real health-check + failover wiring and is
            # delegated to the active (highest-weighted) underlying client.
            assert await client.ping() is True

            # A basic round-trip against the active database.
            assert await client.set("multidb:key", "value") is True
            assert await client.get("multidb:key") == "value"

            # Two real databases are configured.
            assert len(list(client.get_databases())) == 2
