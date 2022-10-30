import asyncio
import binascii
import datetime
import os
import warnings
from typing import Any, Awaitable, Callable, Dict, List, Optional, Type, Union
from urllib.parse import urlparse

import pytest
import pytest_asyncio
from _pytest.fixtures import FixtureRequest

from redis.asyncio.cluster import ClusterNode, NodesManager, RedisCluster
from redis.asyncio.connection import Connection, SSLConnection
from redis.asyncio.parser import CommandsParser
from redis.cluster import PIPELINE_BLOCKED_COMMANDS, PRIMARY, REPLICA, get_node_name
from redis.crc import REDIS_CLUSTER_HASH_SLOTS, key_slot
from redis.exceptions import (
    AskError,
    ClusterDownError,
    ConnectionError,
    DataError,
    MaxConnectionsError,
    MovedError,
    NoPermissionError,
    RedisClusterException,
    RedisError,
    ResponseError,
)
from redis.utils import str_if_bytes
from tests.conftest import (
    skip_if_redis_enterprise,
    skip_if_server_version_lt,
    skip_unless_arch_bits,
)

from .compat import mock

pytestmark = pytest.mark.onlycluster


default_host = "127.0.0.1"
default_port = 7000
default_cluster_slots = [
    [0, 8191, ["127.0.0.1", 7000, "node_0"], ["127.0.0.1", 7003, "node_3"]],
    [8192, 16383, ["127.0.0.1", 7001, "node_1"], ["127.0.0.1", 7002, "node_2"]],
]


@pytest_asyncio.fixture()
async def slowlog(r: RedisCluster) -> None:
    """
    Set the slowlog threshold to 0, and the
    max length to 128. This will force every
    command into the slowlog and allow us
    to test it
    """
    # Save old values
    current_config = await r.config_get(target_nodes=r.get_primaries()[0])
    old_slower_than_value = current_config["slowlog-log-slower-than"]
    old_max_length_value = current_config["slowlog-max-len"]

    # Set the new values
    await r.config_set("slowlog-log-slower-than", 0)
    await r.config_set("slowlog-max-len", 128)

    yield

    await r.config_set("slowlog-log-slower-than", old_slower_than_value)
    await r.config_set("slowlog-max-len", old_max_length_value)


async def get_mocked_redis_client(*args, **kwargs) -> RedisCluster:
    """
    Return a stable RedisCluster object that have deterministic
    nodes and slots setup to remove the problem of different IP addresses
    on different installations and machines.
    """
    cluster_slots = kwargs.pop("cluster_slots", default_cluster_slots)
    coverage_res = kwargs.pop("coverage_result", "yes")
    cluster_enabled = kwargs.pop("cluster_enabled", True)
    with mock.patch.object(ClusterNode, "execute_command") as execute_command_mock:

        async def execute_command(*_args, **_kwargs):
            if _args[0] == "CLUSTER SLOTS":
                mock_cluster_slots = cluster_slots
                return mock_cluster_slots
            elif _args[0] == "COMMAND":
                return {"get": [], "set": []}
            elif _args[0] == "INFO":
                return {"cluster_enabled": cluster_enabled}
            elif len(_args) > 1 and _args[1] == "cluster-require-full-coverage":
                return {"cluster-require-full-coverage": coverage_res}
            else:
                return await execute_command_mock(*_args, **_kwargs)

        execute_command_mock.side_effect = execute_command

        with mock.patch.object(
            CommandsParser, "initialize", autospec=True
        ) as cmd_parser_initialize:

            def cmd_init_mock(self, r: ClusterNode) -> None:
                self.commands = {
                    "GET": {
                        "name": "get",
                        "arity": 2,
                        "flags": ["readonly", "fast"],
                        "first_key_pos": 1,
                        "last_key_pos": 1,
                        "step_count": 1,
                    }
                }

            cmd_parser_initialize.side_effect = cmd_init_mock

            return await RedisCluster(*args, **kwargs)


def mock_node_resp(node: ClusterNode, response: Any) -> ClusterNode:
    connection = mock.AsyncMock()
    connection.is_connected = True
    connection.read_response.return_value = response
    while node._free:
        node._free.pop()
    node._free.append(connection)
    return node


def mock_node_resp_exc(node: ClusterNode, exc: Exception) -> ClusterNode:
    connection = mock.AsyncMock()
    connection.is_connected = True
    connection.read_response.side_effect = exc
    while node._free:
        node._free.pop()
    node._free.append(connection)
    return node


def mock_all_nodes_resp(rc: RedisCluster, response: Any) -> RedisCluster:
    for node in rc.get_nodes():
        mock_node_resp(node, response)
    return rc


async def moved_redirection_helper(
    create_redis: Callable[..., RedisCluster], failover: bool = False
) -> None:
    """
    Test that the client handles MOVED response after a failover.
    Redirection after a failover means that the redirection address is of a
    replica that was promoted to a primary.

    At first call it should return a MOVED ResponseError that will point
    the client to the next server it should talk to.

    Verify that:
    1. it tries to talk to the redirected node
    2. it updates the slot's primary to the redirected node

    For a failover, also verify:
    3. the redirected node's server type updated to 'primary'
    4. the server type of the previous slot owner updated to 'replica'
    """
    rc = await create_redis(cls=RedisCluster, flushdb=False)
    slot = 12182
    redirect_node = None
    # Get the current primary that holds this slot
    prev_primary = rc.nodes_manager.get_node_from_slot(slot)
    if failover:
        if len(rc.nodes_manager.slots_cache[slot]) < 2:
            warnings.warn("Skipping this test since it requires to have a " "replica")
            return
        redirect_node = rc.nodes_manager.slots_cache[slot][1]
    else:
        # Use one of the primaries to be the redirected node
        redirect_node = rc.get_primaries()[0]
    r_host = redirect_node.host
    r_port = redirect_node.port
    with mock.patch.object(
        ClusterNode, "execute_command", autospec=True
    ) as execute_command:

        def moved_redirect_effect(self, *args, **options):
            def ok_response(self, *args, **options):
                assert self.host == r_host
                assert self.port == r_port

                return "MOCK_OK"

            execute_command.side_effect = ok_response
            raise MovedError(f"{slot} {r_host}:{r_port}")

        execute_command.side_effect = moved_redirect_effect
        assert await rc.execute_command("SET", "foo", "bar") == "MOCK_OK"
        slot_primary = rc.nodes_manager.slots_cache[slot][0]
        assert slot_primary == redirect_node
        if failover:
            assert rc.get_node(host=r_host, port=r_port).server_type == PRIMARY
            assert prev_primary.server_type == REPLICA


class TestRedisClusterObj:
    """
    Tests for the RedisCluster class
    """

    async def test_host_port_startup_node(self) -> None:
        """
        Test that it is possible to use host & port arguments as startup node
        args
        """
        cluster = await get_mocked_redis_client(host=default_host, port=default_port)
        assert cluster.get_node(host=default_host, port=default_port) is not None

        await cluster.close()

    async def test_startup_nodes(self) -> None:
        """
        Test that it is possible to use startup_nodes
        argument to init the cluster
        """
        port_1 = 7000
        port_2 = 7001
        startup_nodes = [
            ClusterNode(default_host, port_1),
            ClusterNode(default_host, port_2),
        ]
        cluster = await get_mocked_redis_client(startup_nodes=startup_nodes)
        assert (
            cluster.get_node(host=default_host, port=port_1) is not None
            and cluster.get_node(host=default_host, port=port_2) is not None
        )

        await cluster.close()

        startup_node = ClusterNode("127.0.0.1", 16379)
        async with RedisCluster(startup_nodes=[startup_node], client_name="test") as rc:
            assert await rc.set("A", 1)
            assert await rc.get("A") == b"1"
            assert all(
                [
                    name == "test"
                    for name in (
                        await rc.client_getname(target_nodes=rc.ALL_NODES)
                    ).values()
                ]
            )

    async def test_empty_startup_nodes(self) -> None:
        """
        Test that exception is raised when empty providing empty startup_nodes
        """
        with pytest.raises(RedisClusterException) as ex:
            RedisCluster(startup_nodes=[])

        assert str(ex.value).startswith(
            "RedisCluster requires at least one node to discover the " "cluster"
        ), str_if_bytes(ex.value)

    async def test_from_url(self, request: FixtureRequest) -> None:
        url = request.config.getoption("--redis-url")

        async with RedisCluster.from_url(url) as rc:
            await rc.set("a", 1)
            await rc.get("a") == 1

        rc = RedisCluster.from_url("rediss://localhost:16379")
        assert rc.connection_kwargs["connection_class"] is SSLConnection

    async def test_max_connections(
        self, create_redis: Callable[..., RedisCluster]
    ) -> None:
        rc = await create_redis(cls=RedisCluster, max_connections=10)
        for node in rc.get_nodes():
            assert node.max_connections == 10

        with mock.patch.object(Connection, "read_response") as read_response:

            async def read_response_mocked(*args: Any, **kwargs: Any) -> None:
                await asyncio.sleep(10)

            read_response.side_effect = read_response_mocked

            with pytest.raises(MaxConnectionsError):
                await asyncio.gather(
                    *(
                        rc.ping(target_nodes=RedisCluster.DEFAULT_NODE)
                        for _ in range(11)
                    )
                )

        await rc.close()

    async def test_execute_command_errors(self, r: RedisCluster) -> None:
        """
        Test that if no key is provided then exception should be raised.
        """
        with pytest.raises(RedisClusterException) as ex:
            await r.execute_command("GET")
        assert str(ex.value).startswith(
            "No way to dispatch this command to " "Redis Cluster. Missing key."
        )

    async def test_execute_command_node_flag_primaries(self, r: RedisCluster) -> None:
        """
        Test command execution with nodes flag PRIMARIES
        """
        primaries = r.get_primaries()
        replicas = r.get_replicas()
        mock_all_nodes_resp(r, "PONG")
        assert await r.ping(target_nodes=RedisCluster.PRIMARIES) is True
        for primary in primaries:
            conn = primary._free.pop()
            assert conn.read_response.called is True
        for replica in replicas:
            conn = replica._free.pop()
            assert conn.read_response.called is not True

    async def test_execute_command_node_flag_replicas(self, r: RedisCluster) -> None:
        """
        Test command execution with nodes flag REPLICAS
        """
        replicas = r.get_replicas()
        if not replicas:
            r = await get_mocked_redis_client(default_host, default_port)
        primaries = r.get_primaries()
        mock_all_nodes_resp(r, "PONG")
        assert await r.ping(target_nodes=RedisCluster.REPLICAS) is True
        for replica in replicas:
            conn = replica._free.pop()
            assert conn.read_response.called is True
        for primary in primaries:
            conn = primary._free.pop()
            assert conn.read_response.called is not True

        await r.close()

    async def test_execute_command_node_flag_all_nodes(self, r: RedisCluster) -> None:
        """
        Test command execution with nodes flag ALL_NODES
        """
        mock_all_nodes_resp(r, "PONG")
        assert await r.ping(target_nodes=RedisCluster.ALL_NODES) is True
        for node in r.get_nodes():
            conn = node._free.pop()
            assert conn.read_response.called is True

    async def test_execute_command_node_flag_random(self, r: RedisCluster) -> None:
        """
        Test command execution with nodes flag RANDOM
        """
        mock_all_nodes_resp(r, "PONG")
        assert await r.ping(target_nodes=RedisCluster.RANDOM) is True
        called_count = 0
        for node in r.get_nodes():
            conn = node._free.pop()
            if conn.read_response.called is True:
                called_count += 1
        assert called_count == 1

    async def test_execute_command_default_node(self, r: RedisCluster) -> None:
        """
        Test command execution without node flag is being executed on the
        default node
        """
        def_node = r.get_default_node()
        mock_node_resp(def_node, "PONG")
        assert await r.ping() is True
        conn = def_node._free.pop()
        assert conn.read_response.called

    async def test_ask_redirection(self, r: RedisCluster) -> None:
        """
        Test that the server handles ASK response.

        At first call it should return a ASK ResponseError that will point
        the client to the next server it should talk to.

        Important thing to verify is that it tries to talk to the second node.
        """
        redirect_node = r.get_nodes()[0]
        with mock.patch.object(
            ClusterNode, "execute_command", autospec=True
        ) as execute_command:

            def ask_redirect_effect(self, *args, **options):
                def ok_response(self, *args, **options):
                    assert self.host == redirect_node.host
                    assert self.port == redirect_node.port

                    return "MOCK_OK"

                execute_command.side_effect = ok_response
                raise AskError(f"12182 {redirect_node.host}:{redirect_node.port}")

            execute_command.side_effect = ask_redirect_effect

            assert await r.execute_command("SET", "foo", "bar") == "MOCK_OK"

    async def test_moved_redirection(
        self, create_redis: Callable[..., RedisCluster]
    ) -> None:
        """
        Test that the client handles MOVED response.
        """
        await moved_redirection_helper(create_redis, failover=False)

    async def test_moved_redirection_after_failover(
        self, create_redis: Callable[..., RedisCluster]
    ) -> None:
        """
        Test that the client handles MOVED response after a failover.
        """
        await moved_redirection_helper(create_redis, failover=True)

    async def test_refresh_using_specific_nodes(
        self, create_redis: Callable[..., RedisCluster]
    ) -> None:
        """
        Test making calls on specific nodes when the cluster has failed over to
        another node
        """
        node_7006 = ClusterNode(host=default_host, port=7006, server_type=PRIMARY)
        node_7007 = ClusterNode(host=default_host, port=7007, server_type=PRIMARY)
        with mock.patch.object(
            ClusterNode, "execute_command", autospec=True
        ) as execute_command:
            with mock.patch.object(
                NodesManager, "initialize", autospec=True
            ) as initialize:
                with mock.patch.multiple(
                    Connection,
                    send_packed_command=mock.DEFAULT,
                    connect=mock.DEFAULT,
                    can_read_destructive=mock.DEFAULT,
                ) as mocks:
                    # simulate 7006 as a failed node
                    def execute_command_mock(self, *args, **options):
                        if self.port == 7006:
                            execute_command.failed_calls += 1
                            raise ClusterDownError(
                                "CLUSTERDOWN The cluster is "
                                "down. Use CLUSTER INFO for "
                                "more information"
                            )
                        elif self.port == 7007:
                            execute_command.successful_calls += 1

                    def initialize_mock(self):
                        # start with all slots mapped to 7006
                        self.nodes_cache = {node_7006.name: node_7006}
                        self.default_node = node_7006
                        self.slots_cache = {}

                        for i in range(0, 16383):
                            self.slots_cache[i] = [node_7006]

                        # After the first connection fails, a reinitialize
                        # should follow the cluster to 7007
                        def map_7007(self):
                            self.nodes_cache = {node_7007.name: node_7007}
                            self.default_node = node_7007
                            self.slots_cache = {}

                            for i in range(0, 16383):
                                self.slots_cache[i] = [node_7007]

                        # Change initialize side effect for the second call
                        initialize.side_effect = map_7007

                    execute_command.side_effect = execute_command_mock
                    execute_command.successful_calls = 0
                    execute_command.failed_calls = 0
                    initialize.side_effect = initialize_mock
                    mocks["can_read_destructive"].return_value = False
                    mocks["send_packed_command"].return_value = "MOCK_OK"
                    mocks["connect"].return_value = None
                    with mock.patch.object(
                        CommandsParser, "initialize", autospec=True
                    ) as cmd_parser_initialize:

                        def cmd_init_mock(self, r: ClusterNode) -> None:
                            self.commands = {
                                "GET": {
                                    "name": "get",
                                    "arity": 2,
                                    "flags": ["readonly", "fast"],
                                    "first_key_pos": 1,
                                    "last_key_pos": 1,
                                    "step_count": 1,
                                }
                            }

                        cmd_parser_initialize.side_effect = cmd_init_mock

                        rc = await create_redis(cls=RedisCluster, flushdb=False)
                        assert len(rc.get_nodes()) == 1
                        assert rc.get_node(node_name=node_7006.name) is not None

                        await rc.get("foo")

                        # Cluster should now point to 7007, and there should be
                        # one failed and one successful call
                        assert len(rc.get_nodes()) == 1
                        assert rc.get_node(node_name=node_7007.name) is not None
                        assert rc.get_node(node_name=node_7006.name) is None
                        assert execute_command.failed_calls == 1
                        assert execute_command.successful_calls == 1

    async def test_reading_from_replicas_in_round_robin(self) -> None:
        with mock.patch.multiple(
            Connection,
            send_command=mock.DEFAULT,
            read_response=mock.DEFAULT,
            _connect=mock.DEFAULT,
            can_read_destructive=mock.DEFAULT,
            on_connect=mock.DEFAULT,
        ) as mocks:
            with mock.patch.object(
                ClusterNode, "execute_command", autospec=True
            ) as execute_command:

                async def execute_command_mock_first(self, *args, **options):
                    await self.connection_class(**self.connection_kwargs).connect()
                    # Primary
                    assert self.port == 7001
                    execute_command.side_effect = execute_command_mock_second
                    return "MOCK_OK"

                def execute_command_mock_second(self, *args, **options):
                    # Replica
                    assert self.port == 7002
                    execute_command.side_effect = execute_command_mock_third
                    return "MOCK_OK"

                def execute_command_mock_third(self, *args, **options):
                    # Primary
                    assert self.port == 7001
                    return "MOCK_OK"

                # We don't need to create a real cluster connection but we
                # do want RedisCluster.on_connect function to get called,
                # so we'll mock some of the Connection's functions to allow it
                execute_command.side_effect = execute_command_mock_first
                mocks["send_command"].return_value = True
                mocks["read_response"].return_value = "OK"
                mocks["_connect"].return_value = True
                mocks["can_read_destructive"].return_value = False
                mocks["on_connect"].return_value = True

                # Create a cluster with reading from replications
                read_cluster = await get_mocked_redis_client(
                    host=default_host, port=default_port, read_from_replicas=True
                )
                assert read_cluster.read_from_replicas is True
                # Check that we read from the slot's nodes in a round robin
                # matter.
                # 'foo' belongs to slot 12182 and the slot's nodes are:
                # [(127.0.0.1,7001,primary), (127.0.0.1,7002,replica)]
                await read_cluster.get("foo")
                await read_cluster.get("foo")
                await read_cluster.get("foo")
                mocks["send_command"].assert_has_calls([mock.call("READONLY")])

                await read_cluster.close()

    async def test_keyslot(self, r: RedisCluster) -> None:
        """
        Test that method will compute correct key in all supported cases
        """
        assert r.keyslot("foo") == 12182
        assert r.keyslot("{foo}bar") == 12182
        assert r.keyslot("{foo}") == 12182
        assert r.keyslot(1337) == 4314

        assert r.keyslot(125) == r.keyslot(b"125")
        assert r.keyslot(125) == r.keyslot("\x31\x32\x35")
        assert r.keyslot("大奖") == r.keyslot(b"\xe5\xa4\xa7\xe5\xa5\x96")
        assert r.keyslot("大奖") == r.keyslot(b"\xe5\xa4\xa7\xe5\xa5\x96")
        assert r.keyslot(1337.1234) == r.keyslot("1337.1234")
        assert r.keyslot(1337) == r.keyslot("1337")
        assert r.keyslot(b"abc") == r.keyslot("abc")

    async def test_get_node_name(self) -> None:
        assert (
            get_node_name(default_host, default_port)
            == f"{default_host}:{default_port}"
        )

    async def test_all_nodes(self, r: RedisCluster) -> None:
        """
        Set a list of nodes and it should be possible to iterate over all
        """
        nodes = [node for node in r.nodes_manager.nodes_cache.values()]

        for i, node in enumerate(r.get_nodes()):
            assert node in nodes

    async def test_all_nodes_masters(self, r: RedisCluster) -> None:
        """
        Set a list of nodes with random primaries/replicas config and it shold
        be possible to iterate over all of them.
        """
        nodes = [
            node
            for node in r.nodes_manager.nodes_cache.values()
            if node.server_type == PRIMARY
        ]

        for node in r.get_primaries():
            assert node in nodes

    @pytest.mark.parametrize("error", RedisCluster.ERRORS_ALLOW_RETRY)
    async def test_cluster_down_overreaches_retry_attempts(
        self,
        error: Union[Type[TimeoutError], Type[ClusterDownError], Type[ConnectionError]],
    ) -> None:
        """
        When error that allows retry is thrown, test that we retry executing
        the command as many times as configured in cluster_error_retry_attempts
        and then raise the exception
        """
        with mock.patch.object(RedisCluster, "_execute_command") as execute_command:

            def raise_error(target_node, *args, **kwargs):
                execute_command.failed_calls += 1
                raise error("mocked error")

            execute_command.side_effect = raise_error

            rc = await get_mocked_redis_client(host=default_host, port=default_port)

            with pytest.raises(error):
                await rc.get("bar")
                assert execute_command.failed_calls == rc.cluster_error_retry_attempts

            await rc.close()

    async def test_set_default_node_success(self, r: RedisCluster) -> None:
        """
        test successful replacement of the default cluster node
        """
        default_node = r.get_default_node()
        # get a different node
        new_def_node = None
        for node in r.get_nodes():
            if node != default_node:
                new_def_node = node
                break
        r.set_default_node(new_def_node)
        assert r.get_default_node() == new_def_node

    async def test_set_default_node_failure(self, r: RedisCluster) -> None:
        """
        test failed replacement of the default cluster node
        """
        default_node = r.get_default_node()
        new_def_node = ClusterNode("1.1.1.1", 1111)
        with pytest.raises(DataError):
            r.set_default_node(None)
        with pytest.raises(DataError):
            r.set_default_node(new_def_node)
        assert r.get_default_node() == default_node

    async def test_get_node_from_key(self, r: RedisCluster) -> None:
        """
        Test that get_node_from_key function returns the correct node
        """
        key = "bar"
        slot = r.keyslot(key)
        slot_nodes = r.nodes_manager.slots_cache.get(slot)
        primary = slot_nodes[0]
        assert r.get_node_from_key(key, replica=False) == primary
        replica = r.get_node_from_key(key, replica=True)
        if replica is not None:
            assert replica.server_type == REPLICA
            assert replica in slot_nodes

    @skip_if_redis_enterprise()
    async def test_not_require_full_coverage_cluster_down_error(
        self, r: RedisCluster
    ) -> None:
        """
        When require_full_coverage is set to False (default client config) and not
        all slots are covered, if one of the nodes has 'cluster-require_full_coverage'
        config set to 'yes' some key-based commands should throw ClusterDownError
        """
        node = r.get_node_from_key("foo")
        missing_slot = r.keyslot("foo")
        assert await r.set("foo", "bar") is True
        try:
            assert all(await r.cluster_delslots(missing_slot))
            with pytest.raises(ClusterDownError):
                await r.exists("foo")
        finally:
            try:
                # Add back the missing slot
                assert await r.cluster_addslots(node, missing_slot) is True
                # Make sure we are not getting ClusterDownError anymore
                assert await r.exists("foo") == 1
            except ResponseError as e:
                if f"Slot {missing_slot} is already busy" in str(e):
                    # It can happen if the test failed to delete this slot
                    pass
                else:
                    raise e

    async def test_can_run_concurrent_commands(self, request: FixtureRequest) -> None:
        url = request.config.getoption("--redis-url")
        rc = RedisCluster.from_url(url)
        assert all(
            await asyncio.gather(
                *(rc.echo("i", target_nodes=RedisCluster.ALL_NODES) for i in range(100))
            )
        )
        await rc.close()


class TestClusterRedisCommands:
    """
    Tests for RedisCluster unique commands
    """

    async def test_get_and_set(self, r: RedisCluster) -> None:
        # get and set can't be tested independently of each other
        assert await r.get("a") is None
        byte_string = b"value"
        integer = 5
        unicode_string = chr(3456) + "abcd" + chr(3421)
        assert await r.set("byte_string", byte_string)
        assert await r.set("integer", 5)
        assert await r.set("unicode_string", unicode_string)
        assert await r.get("byte_string") == byte_string
        assert await r.get("integer") == str(integer).encode()
        assert (await r.get("unicode_string")).decode("utf-8") == unicode_string

    async def test_mget_nonatomic(self, r: RedisCluster) -> None:
        assert await r.mget_nonatomic([]) == []
        assert await r.mget_nonatomic(["a", "b"]) == [None, None]
        await r.set("a", "1")
        await r.set("b", "2")
        await r.set("c", "3")

        assert await r.mget_nonatomic("a", "other", "b", "c") == [
            b"1",
            None,
            b"2",
            b"3",
        ]

    async def test_mset_nonatomic(self, r: RedisCluster) -> None:
        d = {"a": b"1", "b": b"2", "c": b"3", "d": b"4"}
        assert await r.mset_nonatomic(d)
        for k, v in d.items():
            assert await r.get(k) == v

    async def test_config_set(self, r: RedisCluster) -> None:
        assert await r.config_set("slowlog-log-slower-than", 0)

    async def test_cluster_config_resetstat(self, r: RedisCluster) -> None:
        await r.ping(target_nodes="all")
        all_info = await r.info(target_nodes="all")
        prior_commands_processed = -1
        for node_info in all_info.values():
            prior_commands_processed = node_info["total_commands_processed"]
            assert prior_commands_processed >= 1
        await r.config_resetstat(target_nodes="all")
        all_info = await r.info(target_nodes="all")
        for node_info in all_info.values():
            reset_commands_processed = node_info["total_commands_processed"]
            assert reset_commands_processed < prior_commands_processed

    async def test_client_setname(self, r: RedisCluster) -> None:
        node = r.get_random_node()
        await r.client_setname("redis_py_test", target_nodes=node)
        client_name = await r.client_getname(target_nodes=node)
        assert client_name == "redis_py_test"

    async def test_exists(self, r: RedisCluster) -> None:
        d = {"a": b"1", "b": b"2", "c": b"3", "d": b"4"}
        await r.mset_nonatomic(d)
        assert await r.exists(*d.keys()) == len(d)

    async def test_delete(self, r: RedisCluster) -> None:
        d = {"a": b"1", "b": b"2", "c": b"3", "d": b"4"}
        await r.mset_nonatomic(d)
        assert await r.delete(*d.keys()) == len(d)
        assert await r.delete(*d.keys()) == 0

    async def test_touch(self, r: RedisCluster) -> None:
        d = {"a": b"1", "b": b"2", "c": b"3", "d": b"4"}
        await r.mset_nonatomic(d)
        assert await r.touch(*d.keys()) == len(d)

    async def test_unlink(self, r: RedisCluster) -> None:
        d = {"a": b"1", "b": b"2", "c": b"3", "d": b"4"}
        await r.mset_nonatomic(d)
        assert await r.unlink(*d.keys()) == len(d)
        # Unlink is non-blocking so we sleep before
        # verifying the deletion
        await asyncio.sleep(0.1)
        assert await r.unlink(*d.keys()) == 0

    async def test_initialize_before_execute_multi_key_command(
        self, request: FixtureRequest
    ) -> None:
        # Test for issue https://github.com/redis/redis-py/issues/2437
        url = request.config.getoption("--redis-url")
        r = RedisCluster.from_url(url)
        assert 0 == await r.exists("a", "b", "c")
        await r.close()

    @skip_if_redis_enterprise()
    async def test_cluster_myid(self, r: RedisCluster) -> None:
        node = r.get_random_node()
        myid = await r.cluster_myid(node)
        assert len(myid) == 40

    @skip_if_redis_enterprise()
    async def test_cluster_slots(self, r: RedisCluster) -> None:
        mock_all_nodes_resp(r, default_cluster_slots)
        cluster_slots = await r.cluster_slots()
        assert isinstance(cluster_slots, dict)
        assert len(default_cluster_slots) == len(cluster_slots)
        assert cluster_slots.get((0, 8191)) is not None
        assert cluster_slots.get((0, 8191)).get("primary") == ("127.0.0.1", 7000)

    @skip_if_redis_enterprise()
    async def test_cluster_addslots(self, r: RedisCluster) -> None:
        node = r.get_random_node()
        mock_node_resp(node, "OK")
        assert await r.cluster_addslots(node, 1, 2, 3) is True

    @skip_if_server_version_lt("7.0.0")
    @skip_if_redis_enterprise()
    async def test_cluster_addslotsrange(self, r: RedisCluster):
        node = r.get_random_node()
        mock_node_resp(node, "OK")
        assert await r.cluster_addslotsrange(node, 1, 5)

    @skip_if_redis_enterprise()
    async def test_cluster_countkeysinslot(self, r: RedisCluster) -> None:
        node = r.nodes_manager.get_node_from_slot(1)
        mock_node_resp(node, 2)
        assert await r.cluster_countkeysinslot(1) == 2

    async def test_cluster_count_failure_report(self, r: RedisCluster) -> None:
        mock_all_nodes_resp(r, 0)
        assert await r.cluster_count_failure_report("node_0") == 0

    @skip_if_redis_enterprise()
    async def test_cluster_delslots(self) -> None:
        cluster_slots = [
            [0, 8191, ["127.0.0.1", 7000, "node_0"]],
            [8192, 16383, ["127.0.0.1", 7001, "node_1"]],
        ]
        r = await get_mocked_redis_client(
            host=default_host, port=default_port, cluster_slots=cluster_slots
        )
        mock_all_nodes_resp(r, "OK")
        node0 = r.get_node(default_host, 7000)
        node1 = r.get_node(default_host, 7001)
        assert await r.cluster_delslots(0, 8192) == [True, True]
        assert node0._free.pop().read_response.called
        assert node1._free.pop().read_response.called

        await r.close()

    @skip_if_server_version_lt("7.0.0")
    @skip_if_redis_enterprise()
    async def test_cluster_delslotsrange(self, r: RedisCluster):
        node = r.get_random_node()
        mock_node_resp(node, "OK")
        await r.cluster_addslots(node, 1, 2, 3, 4, 5)
        assert await r.cluster_delslotsrange(1, 5)

    @skip_if_redis_enterprise()
    async def test_cluster_failover(self, r: RedisCluster) -> None:
        node = r.get_random_node()
        mock_node_resp(node, "OK")
        assert await r.cluster_failover(node) is True
        assert await r.cluster_failover(node, "FORCE") is True
        assert await r.cluster_failover(node, "TAKEOVER") is True
        with pytest.raises(RedisError):
            await r.cluster_failover(node, "FORCT")

    @skip_if_redis_enterprise()
    async def test_cluster_info(self, r: RedisCluster) -> None:
        info = await r.cluster_info()
        assert isinstance(info, dict)
        assert info["cluster_state"] == "ok"

    @skip_if_redis_enterprise()
    async def test_cluster_keyslot(self, r: RedisCluster) -> None:
        mock_all_nodes_resp(r, 12182)
        assert await r.cluster_keyslot("foo") == 12182

    @skip_if_redis_enterprise()
    async def test_cluster_meet(self, r: RedisCluster) -> None:
        node = r.get_default_node()
        mock_node_resp(node, "OK")
        assert await r.cluster_meet("127.0.0.1", 6379) is True

    @skip_if_redis_enterprise()
    async def test_cluster_nodes(self, r: RedisCluster) -> None:
        response = (
            "c8253bae761cb1ecb2b61857d85dfe455a0fec8b 172.17.0.7:7006 "
            "slave aa90da731f673a99617dfe930306549a09f83a6b 0 "
            "1447836263059 5 connected\n"
            "9bd595fe4821a0e8d6b99d70faa660638a7612b3 172.17.0.7:7008 "
            "master - 0 1447836264065 0 connected\n"
            "aa90da731f673a99617dfe930306549a09f83a6b 172.17.0.7:7003 "
            "myself,master - 0 0 2 connected 5461-10922\n"
            "1df047e5a594f945d82fc140be97a1452bcbf93e 172.17.0.7:7007 "
            "slave 19efe5a631f3296fdf21a5441680f893e8cc96ec 0 "
            "1447836262556 3 connected\n"
            "4ad9a12e63e8f0207025eeba2354bcf4c85e5b22 172.17.0.7:7005 "
            "master - 0 1447836262555 7 connected 0-5460\n"
            "19efe5a631f3296fdf21a5441680f893e8cc96ec 172.17.0.7:7004 "
            "master - 0 1447836263562 3 connected 10923-16383\n"
            "fbb23ed8cfa23f17eaf27ff7d0c410492a1093d6 172.17.0.7:7002 "
            "master,fail - 1447829446956 1447829444948 1 disconnected\n"
        )
        mock_all_nodes_resp(r, response)
        nodes = await r.cluster_nodes()
        assert len(nodes) == 7
        assert nodes.get("172.17.0.7:7006") is not None
        assert (
            nodes.get("172.17.0.7:7006").get("node_id")
            == "c8253bae761cb1ecb2b61857d85dfe455a0fec8b"
        )

    @skip_if_redis_enterprise()
    async def test_cluster_nodes_importing_migrating(self, r: RedisCluster) -> None:
        response = (
            "488ead2fcce24d8c0f158f9172cb1f4a9e040fe5 127.0.0.1:16381@26381 "
            "master - 0 1648975557664 3 connected 10923-16383\n"
            "8ae2e70812db80776f739a72374e57fc4ae6f89d 127.0.0.1:16380@26380 "
            "master - 0 1648975555000 2 connected 1 5461-10922 ["
            "2-<-ed8007ccfa2d91a7b76f8e6fba7ba7e257034a16]\n"
            "ed8007ccfa2d91a7b76f8e6fba7ba7e257034a16 127.0.0.1:16379@26379 "
            "myself,master - 0 1648975556000 1 connected 0 2-5460 ["
            "2->-8ae2e70812db80776f739a72374e57fc4ae6f89d]\n"
        )
        mock_all_nodes_resp(r, response)
        nodes = await r.cluster_nodes()
        assert len(nodes) == 3
        node_16379 = nodes.get("127.0.0.1:16379")
        node_16380 = nodes.get("127.0.0.1:16380")
        node_16381 = nodes.get("127.0.0.1:16381")
        assert node_16379.get("migrations") == [
            {
                "slot": "2",
                "node_id": "8ae2e70812db80776f739a72374e57fc4ae6f89d",
                "state": "migrating",
            }
        ]
        assert node_16379.get("slots") == [["0"], ["2", "5460"]]
        assert node_16380.get("migrations") == [
            {
                "slot": "2",
                "node_id": "ed8007ccfa2d91a7b76f8e6fba7ba7e257034a16",
                "state": "importing",
            }
        ]
        assert node_16380.get("slots") == [["1"], ["5461", "10922"]]
        assert node_16381.get("slots") == [["10923", "16383"]]
        assert node_16381.get("migrations") == []

    @skip_if_redis_enterprise()
    async def test_cluster_replicate(self, r: RedisCluster) -> None:
        node = r.get_random_node()
        all_replicas = r.get_replicas()
        mock_all_nodes_resp(r, "OK")
        assert await r.cluster_replicate(node, "c8253bae761cb61857d") is True
        results = await r.cluster_replicate(all_replicas, "c8253bae761cb61857d")
        if isinstance(results, dict):
            for res in results.values():
                assert res is True
        else:
            assert results is True

    @skip_if_redis_enterprise()
    async def test_cluster_reset(self, r: RedisCluster) -> None:
        mock_all_nodes_resp(r, "OK")
        assert await r.cluster_reset() is True
        assert await r.cluster_reset(False) is True
        all_results = await r.cluster_reset(False, target_nodes="all")
        for res in all_results.values():
            assert res is True

    @skip_if_redis_enterprise()
    async def test_cluster_save_config(self, r: RedisCluster) -> None:
        node = r.get_random_node()
        all_nodes = r.get_nodes()
        mock_all_nodes_resp(r, "OK")
        assert await r.cluster_save_config(node) is True
        all_results = await r.cluster_save_config(all_nodes)
        for res in all_results.values():
            assert res is True

    @skip_if_redis_enterprise()
    async def test_cluster_get_keys_in_slot(self, r: RedisCluster) -> None:
        response = ["{foo}1", "{foo}2"]
        node = r.nodes_manager.get_node_from_slot(12182)
        mock_node_resp(node, response)
        keys = await r.cluster_get_keys_in_slot(12182, 4)
        assert keys == response

    @skip_if_redis_enterprise()
    async def test_cluster_set_config_epoch(self, r: RedisCluster) -> None:
        mock_all_nodes_resp(r, "OK")
        assert await r.cluster_set_config_epoch(3) is True
        all_results = await r.cluster_set_config_epoch(3, target_nodes="all")
        for res in all_results.values():
            assert res is True

    @skip_if_redis_enterprise()
    async def test_cluster_setslot(self, r: RedisCluster) -> None:
        node = r.get_random_node()
        mock_node_resp(node, "OK")
        assert await r.cluster_setslot(node, "node_0", 1218, "IMPORTING") is True
        assert await r.cluster_setslot(node, "node_0", 1218, "NODE") is True
        assert await r.cluster_setslot(node, "node_0", 1218, "MIGRATING") is True
        with pytest.raises(RedisError):
            await r.cluster_failover(node, "STABLE")
        with pytest.raises(RedisError):
            await r.cluster_failover(node, "STATE")

    async def test_cluster_setslot_stable(self, r: RedisCluster) -> None:
        node = r.nodes_manager.get_node_from_slot(12182)
        mock_node_resp(node, "OK")
        assert await r.cluster_setslot_stable(12182) is True
        assert node._free.pop().read_response.called

    @skip_if_redis_enterprise()
    async def test_cluster_replicas(self, r: RedisCluster) -> None:
        response = [
            b"01eca22229cf3c652b6fca0d09ff6941e0d2e3 "
            b"127.0.0.1:6377@16377 slave "
            b"52611e796814b78e90ad94be9d769a4f668f9a 0 "
            b"1634550063436 4 connected",
            b"r4xfga22229cf3c652b6fca0d09ff69f3e0d4d "
            b"127.0.0.1:6378@16378 slave "
            b"52611e796814b78e90ad94be9d769a4f668f9a 0 "
            b"1634550063436 4 connected",
        ]
        mock_all_nodes_resp(r, response)
        replicas = await r.cluster_replicas("52611e796814b78e90ad94be9d769a4f668f9a")
        assert replicas.get("127.0.0.1:6377") is not None
        assert replicas.get("127.0.0.1:6378") is not None
        assert (
            replicas.get("127.0.0.1:6378").get("node_id")
            == "r4xfga22229cf3c652b6fca0d09ff69f3e0d4d"
        )

    @skip_if_server_version_lt("7.0.0")
    async def test_cluster_links(self, r: RedisCluster):
        node = r.get_random_node()
        res = await r.cluster_links(node)
        links_to = sum(x.count("to") for x in res)
        links_for = sum(x.count("from") for x in res)
        assert links_to == links_for
        for i in range(0, len(res) - 1, 2):
            assert res[i][3] == res[i + 1][3]

    @skip_if_redis_enterprise()
    async def test_readonly(self) -> None:
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        mock_all_nodes_resp(r, "OK")
        assert await r.readonly() is True
        all_replicas_results = await r.readonly(target_nodes="replicas")
        for res in all_replicas_results.values():
            assert res is True
        for replica in r.get_replicas():
            assert replica._free.pop().read_response.called

        await r.close()

    @skip_if_redis_enterprise()
    async def test_readwrite(self) -> None:
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        mock_all_nodes_resp(r, "OK")
        assert await r.readwrite() is True
        all_replicas_results = await r.readwrite(target_nodes="replicas")
        for res in all_replicas_results.values():
            assert res is True
        for replica in r.get_replicas():
            assert replica._free.pop().read_response.called

        await r.close()

    @skip_if_redis_enterprise()
    async def test_bgsave(self, r: RedisCluster) -> None:
        try:
            assert await r.bgsave()
            await asyncio.sleep(0.3)
            assert await r.bgsave(True)
        except ResponseError as e:
            if "Background save already in progress" not in e.__str__():
                raise

    async def test_info(self, r: RedisCluster) -> None:
        # Map keys to same slot
        await r.set("x{1}", 1)
        await r.set("y{1}", 2)
        await r.set("z{1}", 3)
        # Get node that handles the slot
        slot = r.keyslot("x{1}")
        node = r.nodes_manager.get_node_from_slot(slot)
        # Run info on that node
        info = await r.info(target_nodes=node)
        assert isinstance(info, dict)
        assert info["db0"]["keys"] == 3

    async def _init_slowlog_test(self, r: RedisCluster, node: ClusterNode) -> str:
        slowlog_lim = await r.config_get("slowlog-log-slower-than", target_nodes=node)
        assert (
            await r.config_set("slowlog-log-slower-than", 0, target_nodes=node) is True
        )
        return slowlog_lim["slowlog-log-slower-than"]

    async def _teardown_slowlog_test(
        self, r: RedisCluster, node: ClusterNode, prev_limit: str
    ) -> None:
        assert (
            await r.config_set("slowlog-log-slower-than", prev_limit, target_nodes=node)
            is True
        )

    async def test_slowlog_get(
        self, r: RedisCluster, slowlog: Optional[List[Dict[str, Union[int, bytes]]]]
    ) -> None:
        unicode_string = chr(3456) + "abcd" + chr(3421)
        node = r.get_node_from_key(unicode_string)
        slowlog_limit = await self._init_slowlog_test(r, node)
        assert await r.slowlog_reset(target_nodes=node)
        await r.get(unicode_string)
        slowlog = await r.slowlog_get(target_nodes=node)
        assert isinstance(slowlog, list)
        commands = [log["command"] for log in slowlog]

        get_command = b" ".join((b"GET", unicode_string.encode("utf-8")))
        assert get_command in commands
        assert b"SLOWLOG RESET" in commands

        # the order should be ['GET <uni string>', 'SLOWLOG RESET'],
        # but if other clients are executing commands at the same time, there
        # could be commands, before, between, or after, so just check that
        # the two we care about are in the appropriate order.
        assert commands.index(get_command) < commands.index(b"SLOWLOG RESET")

        # make sure other attributes are typed correctly
        assert isinstance(slowlog[0]["start_time"], int)
        assert isinstance(slowlog[0]["duration"], int)
        # rollback the slowlog limit to its original value
        await self._teardown_slowlog_test(r, node, slowlog_limit)

    async def test_slowlog_get_limit(
        self, r: RedisCluster, slowlog: Optional[List[Dict[str, Union[int, bytes]]]]
    ) -> None:
        assert await r.slowlog_reset()
        node = r.get_node_from_key("foo")
        slowlog_limit = await self._init_slowlog_test(r, node)
        await r.get("foo")
        slowlog = await r.slowlog_get(1, target_nodes=node)
        assert isinstance(slowlog, list)
        # only one command, based on the number we passed to slowlog_get()
        assert len(slowlog) == 1
        await self._teardown_slowlog_test(r, node, slowlog_limit)

    async def test_slowlog_length(self, r: RedisCluster, slowlog: None) -> None:
        await r.get("foo")
        node = r.nodes_manager.get_node_from_slot(key_slot(b"foo"))
        slowlog_len = await r.slowlog_len(target_nodes=node)
        assert isinstance(slowlog_len, int)

    async def test_time(self, r: RedisCluster) -> None:
        t = await r.time(target_nodes=r.get_primaries()[0])
        assert len(t) == 2
        assert isinstance(t[0], int)
        assert isinstance(t[1], int)

    @skip_if_server_version_lt("4.0.0")
    async def test_memory_usage(self, r: RedisCluster) -> None:
        await r.set("foo", "bar")
        assert isinstance(await r.memory_usage("foo"), int)

    @skip_if_server_version_lt("4.0.0")
    @skip_if_redis_enterprise()
    async def test_memory_malloc_stats(self, r: RedisCluster) -> None:
        assert await r.memory_malloc_stats()

    @skip_if_server_version_lt("4.0.0")
    @skip_if_redis_enterprise()
    async def test_memory_stats(self, r: RedisCluster) -> None:
        # put a key into the current db to make sure that "db.<current-db>"
        # has data
        await r.set("foo", "bar")
        node = r.nodes_manager.get_node_from_slot(key_slot(b"foo"))
        stats = await r.memory_stats(target_nodes=node)
        assert isinstance(stats, dict)
        for key, value in stats.items():
            if key.startswith("db."):
                assert isinstance(value, dict)

    @skip_if_server_version_lt("4.0.0")
    async def test_memory_help(self, r: RedisCluster) -> None:
        with pytest.raises(NotImplementedError):
            await r.memory_help()

    @skip_if_server_version_lt("4.0.0")
    async def test_memory_doctor(self, r: RedisCluster) -> None:
        with pytest.raises(NotImplementedError):
            await r.memory_doctor()

    @skip_if_redis_enterprise()
    async def test_lastsave(self, r: RedisCluster) -> None:
        node = r.get_primaries()[0]
        assert isinstance(await r.lastsave(target_nodes=node), datetime.datetime)

    async def test_cluster_echo(self, r: RedisCluster) -> None:
        node = r.get_primaries()[0]
        assert await r.echo("foo bar", target_nodes=node) == b"foo bar"

    @skip_if_server_version_lt("1.0.0")
    async def test_debug_segfault(self, r: RedisCluster) -> None:
        with pytest.raises(NotImplementedError):
            await r.debug_segfault()

    async def test_config_resetstat(self, r: RedisCluster) -> None:
        node = r.get_primaries()[0]
        await r.ping(target_nodes=node)
        prior_commands_processed = int(
            (await r.info(target_nodes=node))["total_commands_processed"]
        )
        assert prior_commands_processed >= 1
        await r.config_resetstat(target_nodes=node)
        reset_commands_processed = int(
            (await r.info(target_nodes=node))["total_commands_processed"]
        )
        assert reset_commands_processed < prior_commands_processed

    @skip_if_server_version_lt("6.2.0")
    async def test_client_trackinginfo(self, r: RedisCluster) -> None:
        node = r.get_primaries()[0]
        res = await r.client_trackinginfo(target_nodes=node)
        assert len(res) > 2
        assert "prefixes" in res

    @skip_if_server_version_lt("2.9.50")
    async def test_client_pause(self, r: RedisCluster) -> None:
        node = r.get_primaries()[0]
        assert await r.client_pause(1, target_nodes=node)
        assert await r.client_pause(timeout=1, target_nodes=node)
        with pytest.raises(RedisError):
            await r.client_pause(timeout="not an integer", target_nodes=node)

    @skip_if_server_version_lt("6.2.0")
    @skip_if_redis_enterprise()
    async def test_client_unpause(self, r: RedisCluster) -> None:
        assert await r.client_unpause()

    @skip_if_server_version_lt("5.0.0")
    async def test_client_id(self, r: RedisCluster) -> None:
        node = r.get_primaries()[0]
        assert await r.client_id(target_nodes=node) > 0

    @skip_if_server_version_lt("5.0.0")
    async def test_client_unblock(self, r: RedisCluster) -> None:
        node = r.get_primaries()[0]
        myid = await r.client_id(target_nodes=node)
        assert not await r.client_unblock(myid, target_nodes=node)
        assert not await r.client_unblock(myid, error=True, target_nodes=node)
        assert not await r.client_unblock(myid, error=False, target_nodes=node)

    @skip_if_server_version_lt("6.0.0")
    async def test_client_getredir(self, r: RedisCluster) -> None:
        node = r.get_primaries()[0]
        assert isinstance(await r.client_getredir(target_nodes=node), int)
        assert await r.client_getredir(target_nodes=node) == -1

    @skip_if_server_version_lt("6.2.0")
    async def test_client_info(self, r: RedisCluster) -> None:
        node = r.get_primaries()[0]
        info = await r.client_info(target_nodes=node)
        assert isinstance(info, dict)
        assert "addr" in info

    @skip_if_server_version_lt("2.6.9")
    async def test_client_kill(self, r: RedisCluster, r2: RedisCluster) -> None:
        node = r.get_primaries()[0]
        await r.client_setname("redis-py-c1", target_nodes="all")
        await r2.client_setname("redis-py-c2", target_nodes="all")
        clients = [
            client
            for client in await r.client_list(target_nodes=node)
            if client.get("name") in ["redis-py-c1", "redis-py-c2"]
        ]
        assert len(clients) == 2
        clients_by_name = {client.get("name"): client for client in clients}

        client_addr = clients_by_name["redis-py-c2"].get("addr")
        assert await r.client_kill(client_addr, target_nodes=node) is True

        clients = [
            client
            for client in await r.client_list(target_nodes=node)
            if client.get("name") in ["redis-py-c1", "redis-py-c2"]
        ]
        assert len(clients) == 1
        assert clients[0].get("name") == "redis-py-c1"

    @skip_if_server_version_lt("2.6.0")
    async def test_cluster_bitop_not_empty_string(self, r: RedisCluster) -> None:
        await r.set("{foo}a", "")
        await r.bitop("not", "{foo}r", "{foo}a")
        assert await r.get("{foo}r") is None

    @skip_if_server_version_lt("2.6.0")
    async def test_cluster_bitop_not(self, r: RedisCluster) -> None:
        test_str = b"\xAA\x00\xFF\x55"
        correct = ~0xAA00FF55 & 0xFFFFFFFF
        await r.set("{foo}a", test_str)
        await r.bitop("not", "{foo}r", "{foo}a")
        assert int(binascii.hexlify(await r.get("{foo}r")), 16) == correct

    @skip_if_server_version_lt("2.6.0")
    async def test_cluster_bitop_not_in_place(self, r: RedisCluster) -> None:
        test_str = b"\xAA\x00\xFF\x55"
        correct = ~0xAA00FF55 & 0xFFFFFFFF
        await r.set("{foo}a", test_str)
        await r.bitop("not", "{foo}a", "{foo}a")
        assert int(binascii.hexlify(await r.get("{foo}a")), 16) == correct

    @skip_if_server_version_lt("2.6.0")
    async def test_cluster_bitop_single_string(self, r: RedisCluster) -> None:
        test_str = b"\x01\x02\xFF"
        await r.set("{foo}a", test_str)
        await r.bitop("and", "{foo}res1", "{foo}a")
        await r.bitop("or", "{foo}res2", "{foo}a")
        await r.bitop("xor", "{foo}res3", "{foo}a")
        assert await r.get("{foo}res1") == test_str
        assert await r.get("{foo}res2") == test_str
        assert await r.get("{foo}res3") == test_str

    @skip_if_server_version_lt("2.6.0")
    async def test_cluster_bitop_string_operands(self, r: RedisCluster) -> None:
        await r.set("{foo}a", b"\x01\x02\xFF\xFF")
        await r.set("{foo}b", b"\x01\x02\xFF")
        await r.bitop("and", "{foo}res1", "{foo}a", "{foo}b")
        await r.bitop("or", "{foo}res2", "{foo}a", "{foo}b")
        await r.bitop("xor", "{foo}res3", "{foo}a", "{foo}b")
        assert int(binascii.hexlify(await r.get("{foo}res1")), 16) == 0x0102FF00
        assert int(binascii.hexlify(await r.get("{foo}res2")), 16) == 0x0102FFFF
        assert int(binascii.hexlify(await r.get("{foo}res3")), 16) == 0x000000FF

    @skip_if_server_version_lt("6.2.0")
    async def test_cluster_copy(self, r: RedisCluster) -> None:
        assert await r.copy("{foo}a", "{foo}b") == 0
        await r.set("{foo}a", "bar")
        assert await r.copy("{foo}a", "{foo}b") == 1
        assert await r.get("{foo}a") == b"bar"
        assert await r.get("{foo}b") == b"bar"

    @skip_if_server_version_lt("6.2.0")
    async def test_cluster_copy_and_replace(self, r: RedisCluster) -> None:
        await r.set("{foo}a", "foo1")
        await r.set("{foo}b", "foo2")
        assert await r.copy("{foo}a", "{foo}b") == 0
        assert await r.copy("{foo}a", "{foo}b", replace=True) == 1

    @skip_if_server_version_lt("6.2.0")
    async def test_cluster_lmove(self, r: RedisCluster) -> None:
        await r.rpush("{foo}a", "one", "two", "three", "four")
        assert await r.lmove("{foo}a", "{foo}b")
        assert await r.lmove("{foo}a", "{foo}b", "right", "left")

    @skip_if_server_version_lt("6.2.0")
    async def test_cluster_blmove(self, r: RedisCluster) -> None:
        await r.rpush("{foo}a", "one", "two", "three", "four")
        assert await r.blmove("{foo}a", "{foo}b", 5)
        assert await r.blmove("{foo}a", "{foo}b", 1, "RIGHT", "LEFT")

    async def test_cluster_msetnx(self, r: RedisCluster) -> None:
        d = {"{foo}a": b"1", "{foo}b": b"2", "{foo}c": b"3"}
        assert await r.msetnx(d)
        d2 = {"{foo}a": b"x", "{foo}d": b"4"}
        assert not await r.msetnx(d2)
        for k, v in d.items():
            assert await r.get(k) == v
        assert await r.get("{foo}d") is None

    async def test_cluster_rename(self, r: RedisCluster) -> None:
        await r.set("{foo}a", "1")
        assert await r.rename("{foo}a", "{foo}b")
        assert await r.get("{foo}a") is None
        assert await r.get("{foo}b") == b"1"

    async def test_cluster_renamenx(self, r: RedisCluster) -> None:
        await r.set("{foo}a", "1")
        await r.set("{foo}b", "2")
        assert not await r.renamenx("{foo}a", "{foo}b")
        assert await r.get("{foo}a") == b"1"
        assert await r.get("{foo}b") == b"2"

    # LIST COMMANDS
    async def test_cluster_blpop(self, r: RedisCluster) -> None:
        await r.rpush("{foo}a", "1", "2")
        await r.rpush("{foo}b", "3", "4")
        assert await r.blpop(["{foo}b", "{foo}a"], timeout=1) == (b"{foo}b", b"3")
        assert await r.blpop(["{foo}b", "{foo}a"], timeout=1) == (b"{foo}b", b"4")
        assert await r.blpop(["{foo}b", "{foo}a"], timeout=1) == (b"{foo}a", b"1")
        assert await r.blpop(["{foo}b", "{foo}a"], timeout=1) == (b"{foo}a", b"2")
        assert await r.blpop(["{foo}b", "{foo}a"], timeout=1) is None
        await r.rpush("{foo}c", "1")
        assert await r.blpop("{foo}c", timeout=1) == (b"{foo}c", b"1")

    async def test_cluster_brpop(self, r: RedisCluster) -> None:
        await r.rpush("{foo}a", "1", "2")
        await r.rpush("{foo}b", "3", "4")
        assert await r.brpop(["{foo}b", "{foo}a"], timeout=1) == (b"{foo}b", b"4")
        assert await r.brpop(["{foo}b", "{foo}a"], timeout=1) == (b"{foo}b", b"3")
        assert await r.brpop(["{foo}b", "{foo}a"], timeout=1) == (b"{foo}a", b"2")
        assert await r.brpop(["{foo}b", "{foo}a"], timeout=1) == (b"{foo}a", b"1")
        assert await r.brpop(["{foo}b", "{foo}a"], timeout=1) is None
        await r.rpush("{foo}c", "1")
        assert await r.brpop("{foo}c", timeout=1) == (b"{foo}c", b"1")

    async def test_cluster_brpoplpush(self, r: RedisCluster) -> None:
        await r.rpush("{foo}a", "1", "2")
        await r.rpush("{foo}b", "3", "4")
        assert await r.brpoplpush("{foo}a", "{foo}b") == b"2"
        assert await r.brpoplpush("{foo}a", "{foo}b") == b"1"
        assert await r.brpoplpush("{foo}a", "{foo}b", timeout=1) is None
        assert await r.lrange("{foo}a", 0, -1) == []
        assert await r.lrange("{foo}b", 0, -1) == [b"1", b"2", b"3", b"4"]

    async def test_cluster_brpoplpush_empty_string(self, r: RedisCluster) -> None:
        await r.rpush("{foo}a", "")
        assert await r.brpoplpush("{foo}a", "{foo}b") == b""

    async def test_cluster_rpoplpush(self, r: RedisCluster) -> None:
        await r.rpush("{foo}a", "a1", "a2", "a3")
        await r.rpush("{foo}b", "b1", "b2", "b3")
        assert await r.rpoplpush("{foo}a", "{foo}b") == b"a3"
        assert await r.lrange("{foo}a", 0, -1) == [b"a1", b"a2"]
        assert await r.lrange("{foo}b", 0, -1) == [b"a3", b"b1", b"b2", b"b3"]

    async def test_cluster_sdiff(self, r: RedisCluster) -> None:
        await r.sadd("{foo}a", "1", "2", "3")
        assert await r.sdiff("{foo}a", "{foo}b") == {b"1", b"2", b"3"}
        await r.sadd("{foo}b", "2", "3")
        assert await r.sdiff("{foo}a", "{foo}b") == {b"1"}

    async def test_cluster_sdiffstore(self, r: RedisCluster) -> None:
        await r.sadd("{foo}a", "1", "2", "3")
        assert await r.sdiffstore("{foo}c", "{foo}a", "{foo}b") == 3
        assert await r.smembers("{foo}c") == {b"1", b"2", b"3"}
        await r.sadd("{foo}b", "2", "3")
        assert await r.sdiffstore("{foo}c", "{foo}a", "{foo}b") == 1
        assert await r.smembers("{foo}c") == {b"1"}

    async def test_cluster_sinter(self, r: RedisCluster) -> None:
        await r.sadd("{foo}a", "1", "2", "3")
        assert await r.sinter("{foo}a", "{foo}b") == set()
        await r.sadd("{foo}b", "2", "3")
        assert await r.sinter("{foo}a", "{foo}b") == {b"2", b"3"}

    async def test_cluster_sinterstore(self, r: RedisCluster) -> None:
        await r.sadd("{foo}a", "1", "2", "3")
        assert await r.sinterstore("{foo}c", "{foo}a", "{foo}b") == 0
        assert await r.smembers("{foo}c") == set()
        await r.sadd("{foo}b", "2", "3")
        assert await r.sinterstore("{foo}c", "{foo}a", "{foo}b") == 2
        assert await r.smembers("{foo}c") == {b"2", b"3"}

    async def test_cluster_smove(self, r: RedisCluster) -> None:
        await r.sadd("{foo}a", "a1", "a2")
        await r.sadd("{foo}b", "b1", "b2")
        assert await r.smove("{foo}a", "{foo}b", "a1")
        assert await r.smembers("{foo}a") == {b"a2"}
        assert await r.smembers("{foo}b") == {b"b1", b"b2", b"a1"}

    async def test_cluster_sunion(self, r: RedisCluster) -> None:
        await r.sadd("{foo}a", "1", "2")
        await r.sadd("{foo}b", "2", "3")
        assert await r.sunion("{foo}a", "{foo}b") == {b"1", b"2", b"3"}

    async def test_cluster_sunionstore(self, r: RedisCluster) -> None:
        await r.sadd("{foo}a", "1", "2")
        await r.sadd("{foo}b", "2", "3")
        assert await r.sunionstore("{foo}c", "{foo}a", "{foo}b") == 3
        assert await r.smembers("{foo}c") == {b"1", b"2", b"3"}

    @skip_if_server_version_lt("6.2.0")
    async def test_cluster_zdiff(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 2, "a3": 3})
        await r.zadd("{foo}b", {"a1": 1, "a2": 2})
        assert await r.zdiff(["{foo}a", "{foo}b"]) == [b"a3"]
        assert await r.zdiff(["{foo}a", "{foo}b"], withscores=True) == [b"a3", b"3"]

    @skip_if_server_version_lt("6.2.0")
    async def test_cluster_zdiffstore(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 2, "a3": 3})
        await r.zadd("{foo}b", {"a1": 1, "a2": 2})
        assert await r.zdiffstore("{foo}out", ["{foo}a", "{foo}b"])
        assert await r.zrange("{foo}out", 0, -1) == [b"a3"]
        assert await r.zrange("{foo}out", 0, -1, withscores=True) == [(b"a3", 3.0)]

    @skip_if_server_version_lt("6.2.0")
    async def test_cluster_zinter(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 2, "a3": 1})
        await r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        await r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert await r.zinter(["{foo}a", "{foo}b", "{foo}c"]) == [b"a3", b"a1"]
        # invalid aggregation
        with pytest.raises(DataError):
            await r.zinter(
                ["{foo}a", "{foo}b", "{foo}c"], aggregate="foo", withscores=True
            )
        # aggregate with SUM
        assert await r.zinter(["{foo}a", "{foo}b", "{foo}c"], withscores=True) == [
            (b"a3", 8),
            (b"a1", 9),
        ]
        # aggregate with MAX
        assert await r.zinter(
            ["{foo}a", "{foo}b", "{foo}c"], aggregate="MAX", withscores=True
        ) == [(b"a3", 5), (b"a1", 6)]
        # aggregate with MIN
        assert await r.zinter(
            ["{foo}a", "{foo}b", "{foo}c"], aggregate="MIN", withscores=True
        ) == [(b"a1", 1), (b"a3", 1)]
        # with weights
        assert await r.zinter(
            {"{foo}a": 1, "{foo}b": 2, "{foo}c": 3}, withscores=True
        ) == [(b"a3", 20), (b"a1", 23)]

    async def test_cluster_zinterstore_sum(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        await r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        await r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert await r.zinterstore("{foo}d", ["{foo}a", "{foo}b", "{foo}c"]) == 2
        assert await r.zrange("{foo}d", 0, -1, withscores=True) == [
            (b"a3", 8),
            (b"a1", 9),
        ]

    async def test_cluster_zinterstore_max(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        await r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        await r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert (
            await r.zinterstore(
                "{foo}d", ["{foo}a", "{foo}b", "{foo}c"], aggregate="MAX"
            )
            == 2
        )
        assert await r.zrange("{foo}d", 0, -1, withscores=True) == [
            (b"a3", 5),
            (b"a1", 6),
        ]

    async def test_cluster_zinterstore_min(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 2, "a3": 3})
        await r.zadd("{foo}b", {"a1": 2, "a2": 3, "a3": 5})
        await r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert (
            await r.zinterstore(
                "{foo}d", ["{foo}a", "{foo}b", "{foo}c"], aggregate="MIN"
            )
            == 2
        )
        assert await r.zrange("{foo}d", 0, -1, withscores=True) == [
            (b"a1", 1),
            (b"a3", 3),
        ]

    async def test_cluster_zinterstore_with_weight(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        await r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        await r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert (
            await r.zinterstore("{foo}d", {"{foo}a": 1, "{foo}b": 2, "{foo}c": 3}) == 2
        )
        assert await r.zrange("{foo}d", 0, -1, withscores=True) == [
            (b"a3", 20),
            (b"a1", 23),
        ]

    @skip_if_server_version_lt("4.9.0")
    async def test_cluster_bzpopmax(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 2})
        await r.zadd("{foo}b", {"b1": 10, "b2": 20})
        assert await r.bzpopmax(["{foo}b", "{foo}a"], timeout=1) == (
            b"{foo}b",
            b"b2",
            20,
        )
        assert await r.bzpopmax(["{foo}b", "{foo}a"], timeout=1) == (
            b"{foo}b",
            b"b1",
            10,
        )
        assert await r.bzpopmax(["{foo}b", "{foo}a"], timeout=1) == (
            b"{foo}a",
            b"a2",
            2,
        )
        assert await r.bzpopmax(["{foo}b", "{foo}a"], timeout=1) == (
            b"{foo}a",
            b"a1",
            1,
        )
        assert await r.bzpopmax(["{foo}b", "{foo}a"], timeout=1) is None
        await r.zadd("{foo}c", {"c1": 100})
        assert await r.bzpopmax("{foo}c", timeout=1) == (b"{foo}c", b"c1", 100)

    @skip_if_server_version_lt("4.9.0")
    async def test_cluster_bzpopmin(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 2})
        await r.zadd("{foo}b", {"b1": 10, "b2": 20})
        assert await r.bzpopmin(["{foo}b", "{foo}a"], timeout=1) == (
            b"{foo}b",
            b"b1",
            10,
        )
        assert await r.bzpopmin(["{foo}b", "{foo}a"], timeout=1) == (
            b"{foo}b",
            b"b2",
            20,
        )
        assert await r.bzpopmin(["{foo}b", "{foo}a"], timeout=1) == (
            b"{foo}a",
            b"a1",
            1,
        )
        assert await r.bzpopmin(["{foo}b", "{foo}a"], timeout=1) == (
            b"{foo}a",
            b"a2",
            2,
        )
        assert await r.bzpopmin(["{foo}b", "{foo}a"], timeout=1) is None
        await r.zadd("{foo}c", {"c1": 100})
        assert await r.bzpopmin("{foo}c", timeout=1) == (b"{foo}c", b"c1", 100)

    @skip_if_server_version_lt("6.2.0")
    async def test_cluster_zrangestore(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 2, "a3": 3})
        assert await r.zrangestore("{foo}b", "{foo}a", 0, 1)
        assert await r.zrange("{foo}b", 0, -1) == [b"a1", b"a2"]
        assert await r.zrangestore("{foo}b", "{foo}a", 1, 2)
        assert await r.zrange("{foo}b", 0, -1) == [b"a2", b"a3"]
        assert await r.zrange("{foo}b", 0, -1, withscores=True) == [
            (b"a2", 2),
            (b"a3", 3),
        ]
        # reversed order
        assert await r.zrangestore("{foo}b", "{foo}a", 1, 2, desc=True)
        assert await r.zrange("{foo}b", 0, -1) == [b"a1", b"a2"]
        # by score
        assert await r.zrangestore(
            "{foo}b", "{foo}a", 2, 1, byscore=True, offset=0, num=1, desc=True
        )
        assert await r.zrange("{foo}b", 0, -1) == [b"a2"]
        # by lex
        assert await r.zrangestore(
            "{foo}b", "{foo}a", "[a2", "(a3", bylex=True, offset=0, num=1
        )
        assert await r.zrange("{foo}b", 0, -1) == [b"a2"]

    @skip_if_server_version_lt("6.2.0")
    async def test_cluster_zunion(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        await r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        await r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        # sum
        assert await r.zunion(["{foo}a", "{foo}b", "{foo}c"]) == [
            b"a2",
            b"a4",
            b"a3",
            b"a1",
        ]
        assert await r.zunion(["{foo}a", "{foo}b", "{foo}c"], withscores=True) == [
            (b"a2", 3),
            (b"a4", 4),
            (b"a3", 8),
            (b"a1", 9),
        ]
        # max
        assert await r.zunion(
            ["{foo}a", "{foo}b", "{foo}c"], aggregate="MAX", withscores=True
        ) == [(b"a2", 2), (b"a4", 4), (b"a3", 5), (b"a1", 6)]
        # min
        assert await r.zunion(
            ["{foo}a", "{foo}b", "{foo}c"], aggregate="MIN", withscores=True
        ) == [(b"a1", 1), (b"a2", 1), (b"a3", 1), (b"a4", 4)]
        # with weight
        assert await r.zunion(
            {"{foo}a": 1, "{foo}b": 2, "{foo}c": 3}, withscores=True
        ) == [(b"a2", 5), (b"a4", 12), (b"a3", 20), (b"a1", 23)]

    async def test_cluster_zunionstore_sum(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        await r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        await r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert await r.zunionstore("{foo}d", ["{foo}a", "{foo}b", "{foo}c"]) == 4
        assert await r.zrange("{foo}d", 0, -1, withscores=True) == [
            (b"a2", 3),
            (b"a4", 4),
            (b"a3", 8),
            (b"a1", 9),
        ]

    async def test_cluster_zunionstore_max(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        await r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        await r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert (
            await r.zunionstore(
                "{foo}d", ["{foo}a", "{foo}b", "{foo}c"], aggregate="MAX"
            )
            == 4
        )
        assert await r.zrange("{foo}d", 0, -1, withscores=True) == [
            (b"a2", 2),
            (b"a4", 4),
            (b"a3", 5),
            (b"a1", 6),
        ]

    async def test_cluster_zunionstore_min(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 2, "a3": 3})
        await r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 4})
        await r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert (
            await r.zunionstore(
                "{foo}d", ["{foo}a", "{foo}b", "{foo}c"], aggregate="MIN"
            )
            == 4
        )
        assert await r.zrange("{foo}d", 0, -1, withscores=True) == [
            (b"a1", 1),
            (b"a2", 2),
            (b"a3", 3),
            (b"a4", 4),
        ]

    async def test_cluster_zunionstore_with_weight(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        await r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        await r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert (
            await r.zunionstore("{foo}d", {"{foo}a": 1, "{foo}b": 2, "{foo}c": 3}) == 4
        )
        assert await r.zrange("{foo}d", 0, -1, withscores=True) == [
            (b"a2", 5),
            (b"a4", 12),
            (b"a3", 20),
            (b"a1", 23),
        ]

    @skip_if_server_version_lt("2.8.9")
    async def test_cluster_pfcount(self, r: RedisCluster) -> None:
        members = {b"1", b"2", b"3"}
        await r.pfadd("{foo}a", *members)
        assert await r.pfcount("{foo}a") == len(members)
        members_b = {b"2", b"3", b"4"}
        await r.pfadd("{foo}b", *members_b)
        assert await r.pfcount("{foo}b") == len(members_b)
        assert await r.pfcount("{foo}a", "{foo}b") == len(members_b.union(members))

    @skip_if_server_version_lt("2.8.9")
    async def test_cluster_pfmerge(self, r: RedisCluster) -> None:
        mema = {b"1", b"2", b"3"}
        memb = {b"2", b"3", b"4"}
        memc = {b"5", b"6", b"7"}
        await r.pfadd("{foo}a", *mema)
        await r.pfadd("{foo}b", *memb)
        await r.pfadd("{foo}c", *memc)
        await r.pfmerge("{foo}d", "{foo}c", "{foo}a")
        assert await r.pfcount("{foo}d") == 6
        await r.pfmerge("{foo}d", "{foo}b")
        assert await r.pfcount("{foo}d") == 7

    async def test_cluster_sort_store(self, r: RedisCluster) -> None:
        await r.rpush("{foo}a", "2", "3", "1")
        assert await r.sort("{foo}a", store="{foo}sorted_values") == 3
        assert await r.lrange("{foo}sorted_values", 0, -1) == [b"1", b"2", b"3"]

    # GEO COMMANDS
    @skip_if_server_version_lt("6.2.0")
    async def test_cluster_geosearchstore(self, r: RedisCluster) -> None:
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        await r.geoadd("{foo}barcelona", values)
        await r.geosearchstore(
            "{foo}places_barcelona",
            "{foo}barcelona",
            longitude=2.191,
            latitude=41.433,
            radius=1000,
        )
        assert await r.zrange("{foo}places_barcelona", 0, -1) == [b"place1"]

    @skip_unless_arch_bits(64)
    @skip_if_server_version_lt("6.2.0")
    async def test_geosearchstore_dist(self, r: RedisCluster) -> None:
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        await r.geoadd("{foo}barcelona", values)
        await r.geosearchstore(
            "{foo}places_barcelona",
            "{foo}barcelona",
            longitude=2.191,
            latitude=41.433,
            radius=1000,
            storedist=True,
        )
        # instead of save the geo score, the distance is saved.
        assert await r.zscore("{foo}places_barcelona", "place1") == 88.05060698409301

    @skip_if_server_version_lt("3.2.0")
    async def test_cluster_georadius_store(self, r: RedisCluster) -> None:
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        await r.geoadd("{foo}barcelona", values)
        await r.georadius(
            "{foo}barcelona", 2.191, 41.433, 1000, store="{foo}places_barcelona"
        )
        assert await r.zrange("{foo}places_barcelona", 0, -1) == [b"place1"]

    @skip_unless_arch_bits(64)
    @skip_if_server_version_lt("3.2.0")
    async def test_cluster_georadius_store_dist(self, r: RedisCluster) -> None:
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        await r.geoadd("{foo}barcelona", values)
        await r.georadius(
            "{foo}barcelona", 2.191, 41.433, 1000, store_dist="{foo}places_barcelona"
        )
        # instead of save the geo score, the distance is saved.
        assert await r.zscore("{foo}places_barcelona", "place1") == 88.05060698409301

    async def test_cluster_dbsize(self, r: RedisCluster) -> None:
        d = {"a": b"1", "b": b"2", "c": b"3", "d": b"4"}
        assert await r.mset_nonatomic(d)
        assert await r.dbsize(target_nodes="primaries") == len(d)

    async def test_cluster_keys(self, r: RedisCluster) -> None:
        assert await r.keys() == []
        keys_with_underscores = {b"test_a", b"test_b"}
        keys = keys_with_underscores.union({b"testc"})
        for key in keys:
            await r.set(key, 1)
        assert (
            set(await r.keys(pattern="test_*", target_nodes="primaries"))
            == keys_with_underscores
        )
        assert set(await r.keys(pattern="test*", target_nodes="primaries")) == keys

    # SCAN COMMANDS
    @skip_if_server_version_lt("2.8.0")
    async def test_cluster_scan(self, r: RedisCluster) -> None:
        await r.set("a", 1)
        await r.set("b", 2)
        await r.set("c", 3)

        for target_nodes, nodes in zip(
            ["primaries", "replicas"], [r.get_primaries(), r.get_replicas()]
        ):
            cursors, keys = await r.scan(target_nodes=target_nodes)
            assert sorted(keys) == [b"a", b"b", b"c"]
            assert sorted(cursors.keys()) == sorted(node.name for node in nodes)
            assert all(cursor == 0 for cursor in cursors.values())

            cursors, keys = await r.scan(match="a*", target_nodes=target_nodes)
            assert sorted(keys) == [b"a"]
            assert sorted(cursors.keys()) == sorted(node.name for node in nodes)
            assert all(cursor == 0 for cursor in cursors.values())

    @skip_if_server_version_lt("6.0.0")
    async def test_cluster_scan_type(self, r: RedisCluster) -> None:
        await r.sadd("a-set", 1)
        await r.sadd("b-set", 1)
        await r.sadd("c-set", 1)
        await r.hset("a-hash", "foo", 2)
        await r.lpush("a-list", "aux", 3)

        for target_nodes, nodes in zip(
            ["primaries", "replicas"], [r.get_primaries(), r.get_replicas()]
        ):
            cursors, keys = await r.scan(_type="SET", target_nodes=target_nodes)
            assert sorted(keys) == [b"a-set", b"b-set", b"c-set"]
            assert sorted(cursors.keys()) == sorted(node.name for node in nodes)
            assert all(cursor == 0 for cursor in cursors.values())

            cursors, keys = await r.scan(
                _type="SET", match="a*", target_nodes=target_nodes
            )
            assert sorted(keys) == [b"a-set"]
            assert sorted(cursors.keys()) == sorted(node.name for node in nodes)
            assert all(cursor == 0 for cursor in cursors.values())

    @skip_if_server_version_lt("2.8.0")
    async def test_cluster_scan_iter(self, r: RedisCluster) -> None:
        keys_all = []
        keys_1 = []
        for i in range(100):
            s = str(i)
            await r.set(s, 1)
            keys_all.append(s.encode("utf-8"))
            if s.startswith("1"):
                keys_1.append(s.encode("utf-8"))
        keys_all.sort()
        keys_1.sort()

        for target_nodes in ["primaries", "replicas"]:
            keys = [key async for key in r.scan_iter(target_nodes=target_nodes)]
            assert sorted(keys) == keys_all

            keys = [
                key async for key in r.scan_iter(match="1*", target_nodes=target_nodes)
            ]
            assert sorted(keys) == keys_1

    async def test_cluster_randomkey(self, r: RedisCluster) -> None:
        node = r.get_node_from_key("{foo}")
        assert await r.randomkey(target_nodes=node) is None
        for key in ("{foo}a", "{foo}b", "{foo}c"):
            await r.set(key, 1)
        assert await r.randomkey(target_nodes=node) in (b"{foo}a", b"{foo}b", b"{foo}c")

    @skip_if_server_version_lt("6.0.0")
    @skip_if_redis_enterprise()
    async def test_acl_log(
        self, r: RedisCluster, create_redis: Callable[..., RedisCluster]
    ) -> None:
        key = "{cache}:"
        node = r.get_node_from_key(key)
        username = "redis-py-user"

        await r.acl_setuser(
            username,
            enabled=True,
            reset=True,
            commands=["+get", "+set", "+select", "+cluster", "+command", "+info"],
            keys=["{cache}:*"],
            nopass=True,
            target_nodes="primaries",
        )
        await r.acl_log_reset(target_nodes=node)

        user_client = await create_redis(
            cls=RedisCluster, flushdb=False, username=username
        )

        # Valid operation and key
        assert await user_client.set("{cache}:0", 1)
        assert await user_client.get("{cache}:0") == b"1"

        # Invalid key
        with pytest.raises(NoPermissionError):
            await user_client.get("{cache}violated_cache:0")

        # Invalid operation
        with pytest.raises(NoPermissionError):
            await user_client.hset("{cache}:0", "hkey", "hval")

        assert isinstance(await r.acl_log(target_nodes=node), list)
        assert len(await r.acl_log(target_nodes=node)) == 2
        assert len(await r.acl_log(count=1, target_nodes=node)) == 1
        assert isinstance((await r.acl_log(target_nodes=node))[0], dict)
        assert "client-info" in (await r.acl_log(count=1, target_nodes=node))[0]
        assert await r.acl_log_reset(target_nodes=node)

        await r.acl_deluser(username, target_nodes="primaries")

        await user_client.close()


class TestNodesManager:
    """
    Tests for the NodesManager class
    """

    async def test_load_balancer(self, r: RedisCluster) -> None:
        n_manager = r.nodes_manager
        lb = n_manager.read_load_balancer
        slot_1 = 1257
        slot_2 = 8975
        node_1 = ClusterNode(default_host, 6379, PRIMARY)
        node_2 = ClusterNode(default_host, 6378, REPLICA)
        node_3 = ClusterNode(default_host, 6377, REPLICA)
        node_4 = ClusterNode(default_host, 6376, PRIMARY)
        node_5 = ClusterNode(default_host, 6375, REPLICA)
        n_manager.slots_cache = {
            slot_1: [node_1, node_2, node_3],
            slot_2: [node_4, node_5],
        }
        primary1_name = n_manager.slots_cache[slot_1][0].name
        primary2_name = n_manager.slots_cache[slot_2][0].name
        list1_size = len(n_manager.slots_cache[slot_1])
        list2_size = len(n_manager.slots_cache[slot_2])
        # slot 1
        assert lb.get_server_index(primary1_name, list1_size) == 0
        assert lb.get_server_index(primary1_name, list1_size) == 1
        assert lb.get_server_index(primary1_name, list1_size) == 2
        assert lb.get_server_index(primary1_name, list1_size) == 0
        # slot 2
        assert lb.get_server_index(primary2_name, list2_size) == 0
        assert lb.get_server_index(primary2_name, list2_size) == 1
        assert lb.get_server_index(primary2_name, list2_size) == 0

        lb.reset()
        assert lb.get_server_index(primary1_name, list1_size) == 0
        assert lb.get_server_index(primary2_name, list2_size) == 0

    async def test_init_slots_cache_not_all_slots_covered(self) -> None:
        """
        Test that if not all slots are covered it should raise an exception
        """
        # Missing slot 5460
        cluster_slots = [
            [0, 5459, ["127.0.0.1", 7000], ["127.0.0.1", 7003]],
            [5461, 10922, ["127.0.0.1", 7001], ["127.0.0.1", 7004]],
            [10923, 16383, ["127.0.0.1", 7002], ["127.0.0.1", 7005]],
        ]
        with pytest.raises(RedisClusterException) as ex:
            rc = await get_mocked_redis_client(
                host=default_host,
                port=default_port,
                cluster_slots=cluster_slots,
                require_full_coverage=True,
            )
            await rc.close()
        assert str(ex.value).startswith(
            "All slots are not covered after query all startup_nodes."
        )

    async def test_init_slots_cache_not_require_full_coverage_success(self) -> None:
        """
        When require_full_coverage is set to False and not all slots are
        covered the cluster client initialization should succeed
        """
        # Missing slot 5460
        cluster_slots = [
            [0, 5459, ["127.0.0.1", 7000], ["127.0.0.1", 7003]],
            [5461, 10922, ["127.0.0.1", 7001], ["127.0.0.1", 7004]],
            [10923, 16383, ["127.0.0.1", 7002], ["127.0.0.1", 7005]],
        ]

        rc = await get_mocked_redis_client(
            host=default_host,
            port=default_port,
            cluster_slots=cluster_slots,
            require_full_coverage=False,
        )

        assert 5460 not in rc.nodes_manager.slots_cache

        await rc.close()

    async def test_init_slots_cache(self) -> None:
        """
        Test that slots cache can in initialized and all slots are covered
        """
        good_slots_resp = [
            [0, 5460, ["127.0.0.1", 7000], ["127.0.0.2", 7003]],
            [5461, 10922, ["127.0.0.1", 7001], ["127.0.0.2", 7004]],
            [10923, 16383, ["127.0.0.1", 7002], ["127.0.0.2", 7005]],
        ]

        rc = await get_mocked_redis_client(
            host=default_host, port=default_port, cluster_slots=good_slots_resp
        )
        n_manager = rc.nodes_manager
        assert len(n_manager.slots_cache) == REDIS_CLUSTER_HASH_SLOTS
        for slot_info in good_slots_resp:
            all_hosts = ["127.0.0.1", "127.0.0.2"]
            all_ports = [7000, 7001, 7002, 7003, 7004, 7005]
            slot_start = slot_info[0]
            slot_end = slot_info[1]
            for i in range(slot_start, slot_end + 1):
                assert len(n_manager.slots_cache[i]) == len(slot_info[2:])
                assert n_manager.slots_cache[i][0].host in all_hosts
                assert n_manager.slots_cache[i][1].host in all_hosts
                assert n_manager.slots_cache[i][0].port in all_ports
                assert n_manager.slots_cache[i][1].port in all_ports

        assert len(n_manager.nodes_cache) == 6

        await rc.close()

    async def test_init_slots_cache_cluster_mode_disabled(self) -> None:
        """
        Test that creating a RedisCluster failes if one of the startup nodes
        has cluster mode disabled
        """
        with pytest.raises(RedisClusterException) as e:
            rc = await get_mocked_redis_client(
                host=default_host, port=default_port, cluster_enabled=False
            )
            await rc.close()
        assert "Cluster mode is not enabled on this node" in str(e.value)

    async def test_empty_startup_nodes(self) -> None:
        """
        It should not be possible to create a node manager with no nodes
        specified
        """
        with pytest.raises(RedisClusterException):
            await NodesManager([], False, {}).initialize()

    async def test_wrong_startup_nodes_type(self) -> None:
        """
        If something other then a list type itteratable is provided it should
        fail
        """
        with pytest.raises(RedisClusterException):
            await NodesManager({}, False, {}).initialize()

    async def test_init_slots_cache_slots_collision(self) -> None:
        """
        Test that if 2 nodes do not agree on the same slots setup it should
        raise an error. In this test both nodes will say that the first
        slots block should be bound to different servers.
        """
        with mock.patch.object(
            ClusterNode, "execute_command", autospec=True
        ) as execute_command:

            async def mocked_execute_command(self, *args, **kwargs):
                """
                Helper function to return custom slots cache data from
                different redis nodes
                """
                if self.port == 7000:
                    result = [
                        [0, 5460, ["127.0.0.1", 7000], ["127.0.0.1", 7003]],
                        [5461, 10922, ["127.0.0.1", 7001], ["127.0.0.1", 7004]],
                    ]

                elif self.port == 7001:
                    result = [
                        [0, 5460, ["127.0.0.1", 7001], ["127.0.0.1", 7003]],
                        [5461, 10922, ["127.0.0.1", 7000], ["127.0.0.1", 7004]],
                    ]
                else:
                    result = []

                if args[0] == "CLUSTER SLOTS":
                    return result
                elif args[0] == "INFO":
                    return {"cluster_enabled": True}
                elif args[1] == "cluster-require-full-coverage":
                    return {"cluster-require-full-coverage": "yes"}

            execute_command.side_effect = mocked_execute_command

            with pytest.raises(RedisClusterException) as ex:
                node_1 = ClusterNode("127.0.0.1", 7000)
                node_2 = ClusterNode("127.0.0.1", 7001)
                async with RedisCluster(startup_nodes=[node_1, node_2]):
                    ...
            assert str(ex.value).startswith(
                "startup_nodes could not agree on a valid slots cache"
            ), str(ex.value)

    async def test_cluster_one_instance(self) -> None:
        """
        If the cluster exists of only 1 node then there is some hacks that must
        be validated they work.
        """
        node = ClusterNode(default_host, default_port)
        cluster_slots = [[0, 16383, ["", default_port]]]
        rc = await get_mocked_redis_client(
            startup_nodes=[node], cluster_slots=cluster_slots
        )

        n = rc.nodes_manager
        assert len(n.nodes_cache) == 1
        n_node = rc.get_node(node_name=node.name)
        assert n_node is not None
        assert n_node == node
        assert n_node.server_type == PRIMARY
        assert len(n.slots_cache) == REDIS_CLUSTER_HASH_SLOTS
        for i in range(0, REDIS_CLUSTER_HASH_SLOTS):
            assert n.slots_cache[i] == [n_node]

        await rc.close()

    async def test_init_with_down_node(self) -> None:
        """
        If I can't connect to one of the nodes, everything should still work.
        But if I can't connect to any of the nodes, exception should be thrown.
        """
        with mock.patch.object(
            ClusterNode, "execute_command", autospec=True
        ) as execute_command:

            async def mocked_execute_command(self, *args, **kwargs):
                if self.port == 7000:
                    raise ConnectionError("mock connection error for 7000")

                if args[0] == "CLUSTER SLOTS":
                    return [
                        [0, 8191, ["127.0.0.1", 7001, "node_1"]],
                        [8192, 16383, ["127.0.0.1", 7002, "node_2"]],
                    ]
                elif args[0] == "INFO":
                    return {"cluster_enabled": True}
                elif args[1] == "cluster-require-full-coverage":
                    return {"cluster-require-full-coverage": "yes"}

            execute_command.side_effect = mocked_execute_command

            node_1 = ClusterNode("127.0.0.1", 7000)
            node_2 = ClusterNode("127.0.0.1", 7001)

            # If all startup nodes fail to connect, connection error should be
            # thrown
            with pytest.raises(RedisClusterException) as e:
                async with RedisCluster(startup_nodes=[node_1]):
                    ...
            assert "Redis Cluster cannot be connected" in str(e.value)

            with mock.patch.object(
                CommandsParser, "initialize", autospec=True
            ) as cmd_parser_initialize:

                def cmd_init_mock(self, r: ClusterNode) -> None:
                    self.commands = {
                        "GET": {
                            "name": "get",
                            "arity": 2,
                            "flags": ["readonly", "fast"],
                            "first_key_pos": 1,
                            "last_key_pos": 1,
                            "step_count": 1,
                        }
                    }

                cmd_parser_initialize.side_effect = cmd_init_mock
                # When at least one startup node is reachable, the cluster
                # initialization should succeeds
                async with RedisCluster(startup_nodes=[node_1, node_2]) as rc:
                    assert rc.get_node(host=default_host, port=7001) is not None
                    assert rc.get_node(host=default_host, port=7002) is not None


class TestClusterPipeline:
    """Tests for the ClusterPipeline class."""

    async def test_blocked_arguments(self, r: RedisCluster) -> None:
        """Test handling for blocked pipeline arguments."""
        with pytest.raises(RedisClusterException) as ex:
            r.pipeline(transaction=True)

        assert str(ex.value) == "transaction is deprecated in cluster mode"

        with pytest.raises(RedisClusterException) as ex:
            r.pipeline(shard_hint=True)

        assert str(ex.value) == "shard_hint is deprecated in cluster mode"

    async def test_blocked_methods(self, r: RedisCluster) -> None:
        """Test handling for blocked pipeline commands."""
        pipeline = r.pipeline()
        for command in PIPELINE_BLOCKED_COMMANDS:
            command = command.replace(" ", "_").lower()
            if command == "mset_nonatomic":
                continue

            with pytest.raises(RedisClusterException) as exc:
                getattr(pipeline, command)()

            assert str(exc.value) == (
                f"ERROR: Calling pipelined function {command} is blocked "
                "when running redis in cluster mode..."
            )

    async def test_empty_stack(self, r: RedisCluster) -> None:
        """If a pipeline is executed with no commands it should return a empty list."""
        p = r.pipeline()
        result = await p.execute()
        assert result == []

    async def test_redis_cluster_pipeline(self, r: RedisCluster) -> None:
        """Test that we can use a pipeline with the RedisCluster class"""
        result = await (
            r.pipeline()
            .set("A", 1)
            .get("A")
            .hset("K", "F", "V")
            .hgetall("K")
            .mset_nonatomic({"A": 2, "B": 3})
            .get("A")
            .get("B")
            .delete("A", "B", "K")
            .execute()
        )
        assert result == [True, b"1", 1, {b"F": b"V"}, True, True, b"2", b"3", 1, 1, 1]

    async def test_multi_key_operation_with_a_single_slot(
        self, r: RedisCluster
    ) -> None:
        """Test multi key operation with a single slot."""
        pipe = r.pipeline()
        pipe.set("a{foo}", 1)
        pipe.set("b{foo}", 2)
        pipe.set("c{foo}", 3)
        pipe.get("a{foo}")
        pipe.get("b{foo}")
        pipe.get("c{foo}")

        res = await pipe.execute()
        assert res == [True, True, True, b"1", b"2", b"3"]

    async def test_multi_key_operation_with_multi_slots(self, r: RedisCluster) -> None:
        """Test multi key operation with more than one slot."""
        pipe = r.pipeline()
        pipe.set("a{foo}", 1)
        pipe.set("b{foo}", 2)
        pipe.set("c{foo}", 3)
        pipe.set("bar", 4)
        pipe.set("bazz", 5)
        pipe.get("a{foo}")
        pipe.get("b{foo}")
        pipe.get("c{foo}")
        pipe.get("bar")
        pipe.get("bazz")
        res = await pipe.execute()
        assert res == [True, True, True, True, True, b"1", b"2", b"3", b"4", b"5"]

    async def test_cluster_down_error(self, r: RedisCluster) -> None:
        """
        Test that the pipeline retries cluster_error_retry_attempts times before raising
        an error.
        """
        key = "foo"
        node = r.get_node_from_key(key, False)

        parse_response_orig = node.parse_response
        with mock.patch.object(
            ClusterNode, "parse_response", autospec=True
        ) as parse_response_mock:

            async def parse_response(
                self, connection: Connection, command: str, **kwargs: Any
            ) -> Any:
                if command == "GET":
                    raise ClusterDownError("error")
                return await parse_response_orig(connection, command, **kwargs)

            parse_response_mock.side_effect = parse_response

            # For each ClusterDownError, we launch 4 commands: INFO, CLUSTER SLOTS,
            # COMMAND, GET. Before any errors, the first 3 commands are already run
            async with r.pipeline() as pipe:
                with pytest.raises(ClusterDownError):
                    await pipe.get(key).execute()

                assert (
                    node.parse_response.await_count
                    == 4 * r.cluster_error_retry_attempts - 3
                )

    async def test_connection_error_not_raised(self, r: RedisCluster) -> None:
        """Test ConnectionError handling with raise_on_error=False."""
        key = "foo"
        node = r.get_node_from_key(key, False)

        parse_response_orig = node.parse_response
        with mock.patch.object(
            ClusterNode, "parse_response", autospec=True
        ) as parse_response_mock:

            async def parse_response(
                self, connection: Connection, command: str, **kwargs: Any
            ) -> Any:
                if command == "GET":
                    raise ConnectionError("error")
                return await parse_response_orig(connection, command, **kwargs)

            parse_response_mock.side_effect = parse_response

            async with r.pipeline() as pipe:
                res = await pipe.get(key).get(key).execute(raise_on_error=False)
                assert node.parse_response.await_count
                assert isinstance(res[0], ConnectionError)

    async def test_connection_error_raised(self, r: RedisCluster) -> None:
        """Test ConnectionError handling with raise_on_error=True."""
        key = "foo"
        node = r.get_node_from_key(key, False)

        parse_response_orig = node.parse_response
        with mock.patch.object(
            ClusterNode, "parse_response", autospec=True
        ) as parse_response_mock:

            async def parse_response(
                self, connection: Connection, command: str, **kwargs: Any
            ) -> Any:
                if command == "GET":
                    raise ConnectionError("error")
                return await parse_response_orig(connection, command, **kwargs)

            parse_response_mock.side_effect = parse_response

            async with r.pipeline() as pipe:
                with pytest.raises(ConnectionError):
                    await pipe.get(key).get(key).execute(raise_on_error=True)

    async def test_asking_error(self, r: RedisCluster) -> None:
        """Test AskError handling."""
        key = "foo"
        first_node = r.get_node_from_key(key, False)
        ask_node = None
        for node in r.get_nodes():
            if node != first_node:
                ask_node = node
                break
        ask_msg = f"{r.keyslot(key)} {ask_node.host}:{ask_node.port}"

        async with r.pipeline() as pipe:
            mock_node_resp_exc(first_node, AskError(ask_msg))
            mock_node_resp(ask_node, "MOCK_OK")
            res = await pipe.get(key).execute()
            assert first_node._free.pop().read_response.await_count
            assert ask_node._free.pop().read_response.await_count
            assert res == ["MOCK_OK"]

    async def test_moved_redirection_on_slave_with_default(
        self, r: RedisCluster
    ) -> None:
        """Test MovedError handling."""
        key = "foo"
        await r.set("foo", "bar")
        # set read_from_replicas to True
        r.read_from_replicas = True
        primary = r.get_node_from_key(key, False)
        moved_error = f"{r.keyslot(key)} {primary.host}:{primary.port}"

        parse_response_orig = primary.parse_response
        with mock.patch.object(
            ClusterNode, "parse_response", autospec=True
        ) as parse_response_mock:

            async def parse_response(
                self, connection: Connection, command: str, **kwargs: Any
            ) -> Any:
                if (
                    command == "GET"
                    and self.host != primary.host
                    and self.port != primary.port
                ):
                    raise MovedError(moved_error)

                return await parse_response_orig(connection, command, **kwargs)

            parse_response_mock.side_effect = parse_response

            async with r.pipeline() as readwrite_pipe:
                assert r.reinitialize_counter == 0
                readwrite_pipe.get(key).get(key)
                assert r.reinitialize_counter == 0
                assert await readwrite_pipe.execute() == [b"bar", b"bar"]

    async def test_readonly_pipeline_from_readonly_client(
        self, r: RedisCluster
    ) -> None:
        """Test that the pipeline uses replicas for read_from_replicas clients."""
        # Create a cluster with reading from replications
        r.read_from_replicas = True
        key = "bar"
        await r.set(key, "foo")

        async with r.pipeline() as pipe:
            mock_all_nodes_resp(r, "MOCK_OK")
            assert await pipe.get(key).get(key).execute() == ["MOCK_OK", "MOCK_OK"]
            slot_nodes = r.nodes_manager.slots_cache[r.keyslot(key)]
            executed_on_replica = False
            for node in slot_nodes:
                if node.server_type == REPLICA:
                    if node._free.pop().read_response.await_count:
                        executed_on_replica = True
                        break
            assert executed_on_replica

    async def test_can_run_concurrent_pipelines(self, r: RedisCluster) -> None:
        """Test that the pipeline can be used concurrently."""
        await asyncio.gather(
            *(self.test_redis_cluster_pipeline(r) for i in range(100)),
            *(self.test_multi_key_operation_with_a_single_slot(r) for i in range(100)),
            *(self.test_multi_key_operation_with_multi_slots(r) for i in range(100)),
        )


@pytest.mark.ssl
class TestSSL:
    """
    Tests for SSL connections.

    This relies on the --redis-ssl-url for building the client and connecting to the
    appropriate port.
    """

    ROOT = os.path.join(os.path.dirname(__file__), "../..")
    CERT_DIR = os.path.abspath(os.path.join(ROOT, "docker", "stunnel", "keys"))
    if not os.path.isdir(CERT_DIR):  # github actions package validation case
        CERT_DIR = os.path.abspath(
            os.path.join(ROOT, "..", "docker", "stunnel", "keys")
        )
        if not os.path.isdir(CERT_DIR):
            raise IOError(f"No SSL certificates found. They should be in {CERT_DIR}")

    SERVER_CERT = os.path.join(CERT_DIR, "server-cert.pem")
    SERVER_KEY = os.path.join(CERT_DIR, "server-key.pem")

    @pytest_asyncio.fixture()
    def create_client(self, request: FixtureRequest) -> Callable[..., RedisCluster]:
        ssl_url = request.config.option.redis_ssl_url
        ssl_host, ssl_port = urlparse(ssl_url)[1].split(":")

        async def _create_client(mocked: bool = True, **kwargs: Any) -> RedisCluster:
            if mocked:
                with mock.patch.object(
                    ClusterNode, "execute_command", autospec=True
                ) as execute_command_mock:

                    async def execute_command(self, *args, **kwargs):
                        if args[0] == "INFO":
                            return {"cluster_enabled": True}
                        if args[0] == "CLUSTER SLOTS":
                            return [[0, 16383, [ssl_host, ssl_port, "ssl_node"]]]
                        if args[0] == "COMMAND":
                            return {
                                "ping": {
                                    "name": "ping",
                                    "arity": -1,
                                    "flags": ["stale", "fast"],
                                    "first_key_pos": 0,
                                    "last_key_pos": 0,
                                    "step_count": 0,
                                }
                            }
                        raise NotImplementedError()

                    execute_command_mock.side_effect = execute_command

                    rc = await RedisCluster(host=ssl_host, port=ssl_port, **kwargs)

                assert len(rc.get_nodes()) == 1
                node = rc.get_default_node()
                assert node.port == int(ssl_port)
                return rc

            return await RedisCluster(host=ssl_host, port=ssl_port, **kwargs)

        return _create_client

    async def test_ssl_connection_without_ssl(
        self, create_client: Callable[..., Awaitable[RedisCluster]]
    ) -> None:
        with pytest.raises(RedisClusterException) as e:
            await create_client(mocked=False, ssl=False)
        e = e.value.__cause__
        assert "Connection closed by server" in str(e)

    async def test_ssl_with_invalid_cert(
        self, create_client: Callable[..., Awaitable[RedisCluster]]
    ) -> None:
        with pytest.raises(RedisClusterException) as e:
            await create_client(mocked=False, ssl=True)
        e = e.value.__cause__.__context__
        assert "SSL: CERTIFICATE_VERIFY_FAILED" in str(e)

    async def test_ssl_connection(
        self, create_client: Callable[..., Awaitable[RedisCluster]]
    ) -> None:
        async with await create_client(ssl=True, ssl_cert_reqs="none") as rc:
            assert await rc.ping()

    async def test_validating_self_signed_certificate(
        self, create_client: Callable[..., Awaitable[RedisCluster]]
    ) -> None:
        async with await create_client(
            ssl=True,
            ssl_ca_certs=self.SERVER_CERT,
            ssl_cert_reqs="required",
            ssl_certfile=self.SERVER_CERT,
            ssl_keyfile=self.SERVER_KEY,
        ) as rc:
            assert await rc.ping()

    async def test_validating_self_signed_string_certificate(
        self, create_client: Callable[..., Awaitable[RedisCluster]]
    ) -> None:
        with open(self.SERVER_CERT) as f:
            cert_data = f.read()

        async with await create_client(
            ssl=True,
            ssl_ca_data=cert_data,
            ssl_cert_reqs="required",
            ssl_certfile=self.SERVER_CERT,
            ssl_keyfile=self.SERVER_KEY,
        ) as rc:
            assert await rc.ping()
