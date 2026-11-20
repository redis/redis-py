"""
Unit tests for custom OTel attributes in the async stack.

Custom attributes are configured globally, on the ``OTelConfig`` passed to
``init()``, and per client, on the ``OTelConfig`` passed as ``otel_config``.
They are added to every metric with the precedence
built-in < global < per-client.
"""

from types import SimpleNamespace
from typing import Any, NamedTuple
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import redis.asyncio as redis
from redis.asyncio.connection import ConnectionPool
from redis.asyncio.multidb.config import DatabaseConfig, MultiDbConfig
from redis.asyncio.observability import recorder
from redis.observability.attributes import (
    DB_CLIENT_CONNECTION_POOL_NAME,
    DB_CLIENT_GEOFAILOVER_FAIL_FROM,
    DB_SYSTEM,
    ERROR_TYPE,
    REDIS_CLIENT_PUBSUB_CHANNEL,
    REDIS_CLIENT_STREAM_NAME,
    SERVER_ADDRESS,
    ConnectionState,
    GeoFailoverReason,
    PubSubDirection,
)
from redis.observability.config import MetricGroup, OTelConfig
from redis.observability.metrics import CloseReason, RedisMetricsCollector
from redis.observability.registry import get_observables_registry_instance

GLOBAL_ATTRIBUTES = {"env": "prod", "shared": "global"}
CLIENT_ATTRIBUTES = {"tenant": "a", "shared": "client", DB_SYSTEM: "other"}


MARKER = "custom-attributes-test"


class _MarkerError(Exception):
    pass


def _database(host):
    database = MagicMock()
    database.client.get_connection_kwargs.return_value = {"host": host, "port": 6379}
    database.weight = 1.0
    return database


_POOL = SimpleNamespace(connection_kwargs={"host": MARKER, "port": 6379})


class RecordCall(NamedTuple):
    """
    A recorder function call and how to find the data point it records.

    The collector under test is installed process-wide, so anything still
    running from earlier tests records to it too. A test therefore looks only
    at the instrument of its metric, and there only at the data points
    carrying the marker attributes (and the marker value, for metrics with no
    attribute a test can set).
    """

    metric: str
    kwargs: dict
    marker: dict
    value: Any = None


# Every recorder function that records a metric
RECORD_CALLS = {
    "record_operation_duration": RecordCall(
        "db.client.operation.duration",
        dict(
            command_name="GET",
            duration_seconds=0.1,
            server_address=MARKER,
            server_port=6379,
        ),
        {SERVER_ADDRESS: MARKER},
    ),
    "record_connection_create_time": RecordCall(
        "db.client.connection.create_time",
        dict(connection_pool=_POOL, duration_seconds=0.1),
        {DB_CLIENT_CONNECTION_POOL_NAME: f"{MARKER}:6379"},
    ),
    "record_connection_count": RecordCall(
        "db.client.connection.count",
        dict(pool_name=MARKER, connection_state=ConnectionState.IDLE),
        {DB_CLIENT_CONNECTION_POOL_NAME: MARKER},
    ),
    "record_connection_timeout": RecordCall(
        "db.client.connection.timeouts",
        dict(pool_name=MARKER),
        {DB_CLIENT_CONNECTION_POOL_NAME: MARKER},
    ),
    "record_connection_wait_time": RecordCall(
        "db.client.connection.wait_time",
        dict(pool_name=MARKER, duration_seconds=0.1),
        {DB_CLIENT_CONNECTION_POOL_NAME: MARKER},
    ),
    "record_connection_closed": RecordCall(
        "redis.client.connection.closed",
        dict(close_reason=CloseReason.ERROR, error_type=_MarkerError("boom")),
        {ERROR_TYPE: "_MarkerError"},
    ),
    "record_connection_relaxed_timeout": RecordCall(
        "redis.client.connection.relaxed_timeout",
        dict(connection_name=MARKER, maint_notification="MIGRATING", relaxed=True),
        {DB_CLIENT_CONNECTION_POOL_NAME: MARKER},
    ),
    "record_connection_handoff": RecordCall(
        "redis.client.connection.handoff",
        dict(pool_name=MARKER),
        {DB_CLIENT_CONNECTION_POOL_NAME: MARKER},
    ),
    "record_error_count": RecordCall(
        "redis.client.errors",
        dict(
            server_address=MARKER,
            server_port=6379,
            network_peer_address=MARKER,
            network_peer_port=6379,
            error_type=_MarkerError("boom"),
            retry_attempts=0,
        ),
        {SERVER_ADDRESS: MARKER},
    ),
    "record_pubsub_message": RecordCall(
        "redis.client.pubsub.messages",
        dict(direction=PubSubDirection.PUBLISH, channel=MARKER),
        {REDIS_CLIENT_PUBSUB_CHANNEL: MARKER},
    ),
    "record_streaming_lag": RecordCall(
        "redis.client.stream.lag",
        dict(lag_seconds=0.1, stream_name=MARKER),
        {REDIS_CLIENT_STREAM_NAME: MARKER},
    ),
    "record_streaming_lag_from_response": RecordCall(
        "redis.client.stream.lag",
        dict(response={MARKER: [[("1-0", {"field": "value"})]]}),
        {REDIS_CLIENT_STREAM_NAME: MARKER},
    ),
    "record_maint_notification_count": RecordCall(
        "redis.client.maintenance.notifications",
        dict(
            server_address=MARKER,
            server_port=6379,
            network_peer_address=MARKER,
            network_peer_port=6379,
            maint_notification="MIGRATING",
        ),
        {SERVER_ADDRESS: MARKER},
    ),
    "record_geo_failover": RecordCall(
        "redis.client.geofailover.failovers",
        dict(
            fail_from=_database(MARKER),
            fail_to=_database("secondary"),
            reason=GeoFailoverReason.AUTOMATIC,
        ),
        {DB_CLIENT_GEOFAILOVER_FAIL_FROM: f"{MARKER}:6379/1.0"},
    ),
}


def _install_collector(config):
    """
    Build a real collector on a mocked meter.

    Returns the collector and the mocked instruments it created, by metric name.
    """
    instruments = {}

    def create_instrument(name, **kwargs):
        instruments[name] = MagicMock()
        return instruments[name]

    meter = MagicMock()
    meter.create_counter.side_effect = create_instrument
    meter.create_histogram.side_effect = create_instrument
    meter.create_up_down_counter.side_effect = create_instrument
    meter.create_observable_gauge.side_effect = create_instrument
    with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
        return RedisMetricsCollector(meter, config), instruments


def _recorded_attributes(instruments, record_call):
    """The attributes of the data points the given call recorded."""
    instrument = instruments[record_call.metric]
    return [
        call.kwargs["attributes"]
        for call in instrument.add.call_args_list + instrument.record.call_args_list
        if record_call.marker.items() <= call.kwargs["attributes"].items()
        and (record_call.value is None or call.args[0] == record_call.value)
    ]


@pytest.fixture
def install_collector():
    """Install a collector built from the given global config into the recorder."""
    recorder.reset_collector()
    get_observables_registry_instance().clear()
    patches = []

    def install(config):
        collector, instruments = _install_collector(config)
        for target, value in (
            ("_async_metrics_collector", collector),
            ("_get_config", AsyncMock(return_value=config)),
        ):
            patcher = patch.object(recorder, target, value)
            patcher.start()
            patches.append(patcher)
        return instruments

    yield install

    for patcher in patches:
        patcher.stop()
    recorder.reset_collector()
    get_observables_registry_instance().clear()


@pytest.mark.fixed_client
@pytest.mark.asyncio
class TestCustomAttributesOnMetrics:
    async def test_every_recorder_function_is_covered(self):
        record_functions = {
            name
            for name in dir(recorder)
            if name.startswith("record_") and callable(getattr(recorder, name))
        }
        assert record_functions == set(RECORD_CALLS)

    @pytest.mark.parametrize("name", RECORD_CALLS)
    async def test_global_attributes(self, install_collector, name):
        instruments = install_collector(
            OTelConfig(
                metric_groups=list(MetricGroup), custom_attributes=GLOBAL_ATTRIBUTES
            )
        )

        await getattr(recorder, name)(**RECORD_CALLS[name].kwargs)

        recorded = _recorded_attributes(instruments, RECORD_CALLS[name])
        assert len(recorded) == 1
        assert recorded[0]["env"] == "prod"
        assert recorded[0]["shared"] == "global"
        assert recorded[0][DB_SYSTEM] == "redis"
        assert "tenant" not in recorded[0]

    @pytest.mark.parametrize("name", RECORD_CALLS)
    async def test_per_client_attributes_win_over_global_and_built_in(
        self, install_collector, name
    ):
        instruments = install_collector(
            OTelConfig(
                metric_groups=list(MetricGroup), custom_attributes=GLOBAL_ATTRIBUTES
            )
        )

        await getattr(recorder, name)(
            **RECORD_CALLS[name].kwargs,
            otel_config=OTelConfig(custom_attributes=CLIENT_ATTRIBUTES),
        )

        recorded = _recorded_attributes(instruments, RECORD_CALLS[name])
        assert len(recorded) == 1
        assert recorded[0]["env"] == "prod"
        assert recorded[0]["tenant"] == "a"
        assert recorded[0]["shared"] == "client"
        assert recorded[0][DB_SYSTEM] == "other"

    @pytest.mark.parametrize("name", RECORD_CALLS)
    async def test_per_client_attributes_without_global_ones(
        self, install_collector, name
    ):
        instruments = install_collector(OTelConfig(metric_groups=list(MetricGroup)))

        await getattr(recorder, name)(
            **RECORD_CALLS[name].kwargs,
            otel_config=OTelConfig(custom_attributes={"tenant": "a"}),
        )

        recorded = _recorded_attributes(instruments, RECORD_CALLS[name])
        assert len(recorded) == 1
        assert recorded[0]["tenant"] == "a"
        assert recorded[0][DB_SYSTEM] == "redis"

    @pytest.mark.parametrize("name", RECORD_CALLS)
    @pytest.mark.parametrize(
        "otel_config", [None, OTelConfig()], ids=["no-config", "empty-config"]
    )
    async def test_attributes_are_unchanged_without_custom_ones(
        self, install_collector, name, otel_config
    ):
        instruments = install_collector(OTelConfig(metric_groups=list(MetricGroup)))
        await getattr(recorder, name)(**RECORD_CALLS[name].kwargs)
        expected = _recorded_attributes(instruments, RECORD_CALLS[name])

        instruments = install_collector(OTelConfig(metric_groups=list(MetricGroup)))
        await getattr(recorder, name)(
            **RECORD_CALLS[name].kwargs, otel_config=otel_config
        )

        assert _recorded_attributes(instruments, RECORD_CALLS[name]) == expected

    async def test_a_config_of_another_type_is_ignored(self, install_collector):
        instruments = install_collector(
            OTelConfig(
                metric_groups=list(MetricGroup), custom_attributes=GLOBAL_ATTRIBUTES
            )
        )

        await recorder.record_connection_handoff(pool_name=MARKER, otel_config=object())

        recorded = _recorded_attributes(
            instruments, RECORD_CALLS["record_connection_handoff"]
        )
        assert len(recorded) == 1
        assert recorded[0]["env"] == "prod"


@pytest.mark.fixed_client
@pytest.mark.asyncio
class TestOTelConfigPropagation:
    async def test_client_has_no_config_by_default(self):
        client = redis.Redis()

        assert client.otel_config is None
        assert client.connection_pool.otel_config is None
        assert client.connection_pool.make_connection().otel_config is None

    async def test_client_config_reaches_pool_and_connections(self):
        config = OTelConfig(custom_attributes=CLIENT_ATTRIBUTES)
        client = redis.Redis(otel_config=config)

        assert client.otel_config is config
        assert client.connection_pool.otel_config is config
        assert client.connection_pool.make_connection().otel_config is config
        assert client.pipeline().otel_config is config
        assert client.pubsub().otel_config is config

    async def test_from_url(self):
        config = OTelConfig(custom_attributes=CLIENT_ATTRIBUTES)
        client = redis.Redis.from_url("redis://localhost:6379/0", otel_config=config)

        assert client.otel_config is config
        assert client.connection_pool.make_connection().otel_config is config

    async def test_a_supplied_pool_keeps_its_own_config(self):
        pool_config = OTelConfig(custom_attributes=CLIENT_ATTRIBUTES)
        pool = ConnectionPool(otel_config=pool_config)
        client = redis.Redis(
            connection_pool=pool,
            otel_config=OTelConfig(custom_attributes={"ignored": "yes"}),
        )

        assert client.otel_config is pool_config

    async def test_multidb_databases_inherit_the_config(self):
        config = OTelConfig(custom_attributes=CLIENT_ATTRIBUTES)
        own_config = OTelConfig(custom_attributes={"tenant": "b"})
        pool_config = OTelConfig(custom_attributes={"tenant": "c"})
        multidb_config = MultiDbConfig(
            databases_config=[
                DatabaseConfig(weight=1.0, client_kwargs={"host": "db1"}),
                DatabaseConfig(weight=0.9, from_url="redis://db2:6379"),
                DatabaseConfig(
                    weight=0.8,
                    client_kwargs={"host": "db3", "otel_config": own_config},
                ),
                DatabaseConfig(
                    weight=0.7,
                    from_pool=ConnectionPool(host="db4", otel_config=pool_config),
                ),
            ],
            otel_config=config,
        )

        configs = {
            database.client.get_connection_kwargs()["host"]: database.client.otel_config
            for database, _ in multidb_config.databases()
        }

        assert configs == {
            "db1": config,
            "db2": config,
            "db3": own_config,
            "db4": pool_config,
        }

    async def test_client_metrics_carry_the_client_attributes(self, install_collector):
        instruments = install_collector(
            OTelConfig(
                metric_groups=[MetricGroup.COMMAND],
                custom_attributes=GLOBAL_ATTRIBUTES,
            )
        )
        with_config = redis.Redis(
            otel_config=OTelConfig(custom_attributes=CLIENT_ATTRIBUTES)
        )
        without_config = redis.Redis()

        for client in (with_config, without_config):
            connection = MagicMock()
            connection.host = MARKER
            connection.port = 6379
            connection.db = 0
            connection.should_reconnect.return_value = False
            connection.retry.call_with_retry = lambda do, fail, **kwargs: do()
            with (
                patch.object(
                    client.connection_pool,
                    "get_connection",
                    AsyncMock(return_value=connection),
                ),
                patch.object(client.connection_pool, "release", AsyncMock()),
                patch.object(
                    client,
                    "_send_command_parse_response",
                    AsyncMock(return_value=True),
                ),
            ):
                await client.execute_command("SET", "key", "value")

        first, second = _recorded_attributes(
            instruments, RECORD_CALLS["record_operation_duration"]
        )
        assert first["tenant"] == "a"
        assert first["shared"] == "client"
        assert second["shared"] == "global"
        assert "tenant" not in second


@pytest.mark.fixed_client
@pytest.mark.asyncio
class TestOTelConfigClusterPropagation:
    async def test_cluster_config_reaches_every_node(self):
        config = OTelConfig(custom_attributes=CLIENT_ATTRIBUTES)
        cluster = redis.RedisCluster(host="localhost", port=16379, otel_config=config)

        assert cluster.otel_config is config
        assert cluster.pipeline().otel_config is config
        nodes = list(cluster.nodes_manager.startup_nodes.values())
        assert nodes
        for node in nodes:
            assert node.connection_kwargs["otel_config"] is config
            assert node.connection_class(**node.connection_kwargs).otel_config is config
        await cluster.aclose()

    async def test_cluster_has_no_config_by_default(self):
        cluster = redis.RedisCluster(host="localhost", port=16379)

        assert cluster.otel_config is None
        await cluster.aclose()
