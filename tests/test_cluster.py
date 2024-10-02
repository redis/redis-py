import binascii
import datetime
import select
import socket
import socketserver
import threading
import warnings
from queue import LifoQueue, Queue
from time import sleep
from unittest.mock import DEFAULT, Mock, call, patch

import pytest
import redis
from redis import Redis
from redis._parsers import CommandsParser
from redis.backoff import ExponentialBackoff, NoBackoff, default_backoff
from redis.cluster import (
    PRIMARY,
    REDIS_CLUSTER_HASH_SLOTS,
    REPLICA,
    ClusterNode,
    NodesManager,
    RedisCluster,
    get_node_name,
)
from redis.connection import BlockingConnectionPool, Connection, ConnectionPool
from redis.crc import key_slot
from redis.exceptions import (
    AskError,
    ClusterDownError,
    ConnectionError,
    DataError,
    MovedError,
    NoPermissionError,
    RedisClusterException,
    RedisError,
    ResponseError,
    TimeoutError,
)
from redis.retry import Retry
from redis.utils import str_if_bytes
from tests.test_pubsub import wait_for_message

from .conftest import (
    _get_client,
    assert_resp_response,
    is_resp2_connection,
    skip_if_redis_enterprise,
    skip_if_server_version_lt,
    skip_unless_arch_bits,
    wait_for_command,
)

default_host = "127.0.0.1"
default_port = 7000
default_cluster_slots = [
    [0, 8191, ["127.0.0.1", 7000, "node_0"], ["127.0.0.1", 7003, "node_3"]],
    [8192, 16383, ["127.0.0.1", 7001, "node_1"], ["127.0.0.1", 7002, "node_2"]],
]


class ProxyRequestHandler(socketserver.BaseRequestHandler):
    def recv(self, sock):
        """A recv with a timeout"""
        r = select.select([sock], [], [], 0.01)
        if not r[0]:
            return None
        return sock.recv(1000)

    def handle(self):
        self.server.proxy.n_connections += 1
        conn = socket.create_connection(self.server.proxy.redis_addr)
        stop = False

        def from_server():
            # read from server and pass to client
            while not stop:
                data = self.recv(conn)
                if data is None:
                    continue
                if not data:
                    self.request.shutdown(socket.SHUT_WR)
                    return
                self.request.sendall(data)

        thread = threading.Thread(target=from_server)
        thread.start()
        try:
            while True:
                # read from client and send to server
                data = self.request.recv(1000)
                if not data:
                    return
                conn.sendall(data)
        finally:
            conn.shutdown(socket.SHUT_WR)
            stop = True  # for safety
            thread.join()
            conn.close()


class NodeProxy:
    """A class to proxy a node connection to a different port"""

    def __init__(self, addr, redis_addr):
        self.addr = addr
        self.redis_addr = redis_addr
        self.server = socketserver.ThreadingTCPServer(self.addr, ProxyRequestHandler)
        self.server.proxy = self
        self.server.socket_reuse_address = True
        self.thread = None
        self.n_connections = 0

    def start(self):
        # test that we can connect to redis
        s = socket.create_connection(self.redis_addr, timeout=2)
        s.close()
        # Start a thread with the server -- that thread will then start one
        # more thread for each request
        self.thread = threading.Thread(target=self.server.serve_forever)
        # Exit the server thread when the main thread terminates
        self.thread.daemon = True
        self.thread.start()

    def close(self):
        self.server.shutdown()


@pytest.fixture()
def slowlog(request, r):
    """
    Set the slowlog threshold to 0, and the
    max length to 128. This will force every
    command into the slowlog and allow us
    to test it
    """
    # Save old values
    current_config = r.config_get(target_nodes=r.get_primaries()[0])
    old_slower_than_value = current_config["slowlog-log-slower-than"]
    old_max_legnth_value = current_config["slowlog-max-len"]

    # Function to restore the old values
    def cleanup():
        r.config_set("slowlog-log-slower-than", old_slower_than_value)
        r.config_set("slowlog-max-len", old_max_legnth_value)

    request.addfinalizer(cleanup)

    # Set the new values
    r.config_set("slowlog-log-slower-than", 0)
    r.config_set("slowlog-max-len", 128)


def get_mocked_redis_client(
    func=None, cluster_slots_raise_error=False, *args, **kwargs
):
    """
    Return a stable RedisCluster object that have deterministic
    nodes and slots setup to remove the problem of different IP addresses
    on different installations and machines.
    """
    cluster_slots = kwargs.pop("cluster_slots", default_cluster_slots)
    coverage_res = kwargs.pop("coverage_result", "yes")
    cluster_enabled = kwargs.pop("cluster_enabled", True)
    with patch.object(Redis, "execute_command") as execute_command_mock:

        def execute_command(*_args, **_kwargs):
            if _args[0] == "CLUSTER SLOTS":
                if cluster_slots_raise_error:
                    raise ResponseError()
                else:
                    mock_cluster_slots = cluster_slots
                    return mock_cluster_slots
            elif _args[0] == "COMMAND":
                return {"get": [], "set": []}
            elif _args[0] == "INFO":
                return {"cluster_enabled": cluster_enabled}
            elif len(_args) > 1 and _args[1] == "cluster-require-full-coverage":
                return {"cluster-require-full-coverage": coverage_res}
            elif func is not None:
                return func(*args, **kwargs)
            else:
                return execute_command_mock(*_args, **_kwargs)

        execute_command_mock.side_effect = execute_command

        with patch.object(
            CommandsParser, "initialize", autospec=True
        ) as cmd_parser_initialize:

            def cmd_init_mock(self, r):
                self.commands = {
                    "get": {
                        "name": "get",
                        "arity": 2,
                        "flags": ["readonly", "fast"],
                        "first_key_pos": 1,
                        "last_key_pos": 1,
                        "step_count": 1,
                    }
                }

            cmd_parser_initialize.side_effect = cmd_init_mock

            return RedisCluster(*args, **kwargs)


def mock_node_resp(node, response):
    connection = Mock()
    connection.read_response.return_value = response
    node.redis_connection.connection = connection
    return node


def mock_node_resp_func(node, func):
    connection = Mock()
    connection.read_response.side_effect = func
    node.redis_connection.connection = connection
    return node


def mock_all_nodes_resp(rc, response):
    for node in rc.get_nodes():
        mock_node_resp(node, response)
    return rc


def find_node_ip_based_on_port(cluster_client, port):
    for node in cluster_client.get_nodes():
        if node.port == port:
            return node.host


def moved_redirection_helper(request, failover=False):
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
    rc = _get_client(RedisCluster, request, flushdb=False)
    slot = 12182
    redirect_node = None
    # Get the current primary that holds this slot
    prev_primary = rc.nodes_manager.get_node_from_slot(slot)
    if failover:
        if len(rc.nodes_manager.slots_cache[slot]) < 2:
            warnings.warn("Skipping this test since it requires to have a replica")
            return
        redirect_node = rc.nodes_manager.slots_cache[slot][1]
    else:
        # Use one of the primaries to be the redirected node
        redirect_node = rc.get_primaries()[0]
    r_host = redirect_node.host
    r_port = redirect_node.port
    with patch.object(Redis, "parse_response") as parse_response:

        def moved_redirect_effect(connection, *args, **options):
            def ok_response(connection, *args, **options):
                assert connection.host == r_host
                assert connection.port == r_port

                return "MOCK_OK"

            parse_response.side_effect = ok_response
            raise MovedError(f"{slot} {r_host}:{r_port}")

        parse_response.side_effect = moved_redirect_effect
        assert rc.execute_command("SET", "foo", "bar") == "MOCK_OK"
        slot_primary = rc.nodes_manager.slots_cache[slot][0]
        assert slot_primary == redirect_node
        if failover:
            assert rc.get_node(host=r_host, port=r_port).server_type == PRIMARY
            assert prev_primary.server_type == REPLICA


@pytest.mark.onlycluster
class TestRedisClusterObj:
    """
    Tests for the RedisCluster class
    """

    def test_host_port_startup_node(self):
        """
        Test that it is possible to use host & port arguments as startup node
        args
        """
        cluster = get_mocked_redis_client(host=default_host, port=default_port)
        assert cluster.get_node(host=default_host, port=default_port) is not None

    def test_startup_nodes(self):
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
        cluster = get_mocked_redis_client(startup_nodes=startup_nodes)
        assert (
            cluster.get_node(host=default_host, port=port_1) is not None
            and cluster.get_node(host=default_host, port=port_2) is not None
        )

    def test_empty_startup_nodes(self):
        """
        Test that exception is raised when empty providing empty startup_nodes
        """
        with pytest.raises(RedisClusterException) as ex:
            RedisCluster(startup_nodes=[])

        assert str(ex.value).startswith(
            "RedisCluster requires at least one node to discover the cluster"
        ), str_if_bytes(ex.value)

    def test_from_url(self, r):
        redis_url = f"redis://{default_host}:{default_port}/0"
        with patch.object(RedisCluster, "from_url") as from_url:

            def from_url_mocked(_url, **_kwargs):
                return get_mocked_redis_client(url=_url, **_kwargs)

            from_url.side_effect = from_url_mocked
            cluster = RedisCluster.from_url(redis_url)
        assert cluster.get_node(host=default_host, port=default_port) is not None

    def test_execute_command_errors(self, r):
        """
        Test that if no key is provided then exception should be raised.
        """
        with pytest.raises(RedisClusterException) as ex:
            r.execute_command("GET")
        assert str(ex.value).startswith(
            "No way to dispatch this command to Redis Cluster. Missing key."
        )

    def test_execute_command_node_flag_primaries(self, r):
        """
        Test command execution with nodes flag PRIMARIES
        """
        primaries = r.get_primaries()
        replicas = r.get_replicas()
        mock_all_nodes_resp(r, "PONG")
        assert r.ping(target_nodes=RedisCluster.PRIMARIES) is True
        for primary in primaries:
            conn = primary.redis_connection.connection
            assert conn.read_response.called is True
        for replica in replicas:
            conn = replica.redis_connection.connection
            assert conn.read_response.called is not True

    def test_execute_command_node_flag_replicas(self, r):
        """
        Test command execution with nodes flag REPLICAS
        """
        replicas = r.get_replicas()
        if not replicas:
            r = get_mocked_redis_client(default_host, default_port)
        primaries = r.get_primaries()
        mock_all_nodes_resp(r, "PONG")
        assert r.ping(target_nodes=RedisCluster.REPLICAS) is True
        for replica in replicas:
            conn = replica.redis_connection.connection
            assert conn.read_response.called is True
        for primary in primaries:
            conn = primary.redis_connection.connection
            assert conn.read_response.called is not True

    def test_execute_command_node_flag_all_nodes(self, r):
        """
        Test command execution with nodes flag ALL_NODES
        """
        mock_all_nodes_resp(r, "PONG")
        assert r.ping(target_nodes=RedisCluster.ALL_NODES) is True
        for node in r.get_nodes():
            conn = node.redis_connection.connection
            assert conn.read_response.called is True

    def test_execute_command_node_flag_random(self, r):
        """
        Test command execution with nodes flag RANDOM
        """
        mock_all_nodes_resp(r, "PONG")
        assert r.ping(target_nodes=RedisCluster.RANDOM) is True
        called_count = 0
        for node in r.get_nodes():
            conn = node.redis_connection.connection
            if conn.read_response.called is True:
                called_count += 1
        assert called_count == 1

    def test_execute_command_default_node(self, r):
        """
        Test command execution without node flag is being executed on the
        default node
        """
        def_node = r.get_default_node()
        mock_node_resp(def_node, "PONG")
        assert r.ping() is True
        conn = def_node.redis_connection.connection
        assert conn.read_response.called

    def test_ask_redirection(self, r):
        """
        Test that the server handles ASK response.

        At first call it should return a ASK ResponseError that will point
        the client to the next server it should talk to.

        Important thing to verify is that it tries to talk to the second node.
        """
        redirect_node = r.get_nodes()[0]
        with patch.object(Redis, "parse_response") as parse_response:

            def ask_redirect_effect(connection, *args, **options):
                def ok_response(connection, *args, **options):
                    assert connection.host == redirect_node.host
                    assert connection.port == redirect_node.port

                    return "MOCK_OK"

                parse_response.side_effect = ok_response
                raise AskError(f"12182 {redirect_node.host}:{redirect_node.port}")

            parse_response.side_effect = ask_redirect_effect

            assert r.execute_command("SET", "foo", "bar") == "MOCK_OK"

    def test_handling_cluster_failover_to_a_replica(self, r):
        # Set the key we'll test for
        key = "key"
        r.set("key", "value")
        primary = r.get_node_from_key(key, replica=False)
        assert str_if_bytes(r.get("key")) == "value"
        # Get the current output of cluster slots
        cluster_slots = primary.redis_connection.execute_command("CLUSTER SLOTS")
        replica_host = ""
        replica_port = 0
        # Replace one of the replicas to be the new primary based on the
        # cluster slots output
        for slot_range in cluster_slots:
            primary_port = slot_range[2][1]
            if primary_port == primary.port:
                if len(slot_range) <= 3:
                    # cluster doesn't have a replica, return
                    return
                replica_host = str_if_bytes(slot_range[3][0])
                replica_port = slot_range[3][1]
                # replace replica and primary in the cluster slots output
                tmp_node = slot_range[2]
                slot_range[2] = slot_range[3]
                slot_range[3] = tmp_node
                break

        def raise_connection_error():
            raise ConnectionError("error")

        def mock_execute_command(*_args, **_kwargs):
            if _args[0] == "CLUSTER SLOTS":
                return cluster_slots
            else:
                raise Exception("Failed to mock cluster slots")

        # Mock connection error for the current primary
        mock_node_resp_func(primary, raise_connection_error)
        primary.redis_connection.set_retry(Retry(NoBackoff(), 1))

        # Mock the cluster slots response for all other nodes
        redis_mock_node = Mock()
        redis_mock_node.execute_command.side_effect = mock_execute_command
        # Mock response value for all other commands
        redis_mock_node.parse_response.return_value = "MOCK_OK"
        for node in r.get_nodes():
            if node.port != primary.port:
                node.redis_connection = redis_mock_node

        assert r.get(key) == "MOCK_OK"
        new_primary = r.get_node_from_key(key, replica=False)
        assert new_primary.host == replica_host
        assert new_primary.port == replica_port
        assert r.get_node(primary.host, primary.port).server_type == REPLICA

    def test_moved_redirection(self, request):
        """
        Test that the client handles MOVED response.
        """
        moved_redirection_helper(request, failover=False)

    def test_moved_redirection_after_failover(self, request):
        """
        Test that the client handles MOVED response after a failover.
        """
        moved_redirection_helper(request, failover=True)

    def test_refresh_using_specific_nodes(self, request):
        """
        Test making calls on specific nodes when the cluster has failed over to
        another node
        """
        node_7006 = ClusterNode(host=default_host, port=7006, server_type=PRIMARY)
        node_7007 = ClusterNode(host=default_host, port=7007, server_type=PRIMARY)
        with patch.object(Redis, "parse_response") as parse_response:
            with patch.object(NodesManager, "initialize", autospec=True) as initialize:
                with patch.multiple(
                    Connection, send_command=DEFAULT, connect=DEFAULT, can_read=DEFAULT
                ) as mocks:
                    # simulate 7006 as a failed node
                    def parse_response_mock(connection, command_name, **options):
                        if connection.port == 7006:
                            parse_response.failed_calls += 1
                            raise ClusterDownError(
                                "CLUSTERDOWN The cluster is "
                                "down. Use CLUSTER INFO for "
                                "more information"
                            )
                        elif connection.port == 7007:
                            parse_response.successful_calls += 1

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

                    parse_response.side_effect = parse_response_mock
                    parse_response.successful_calls = 0
                    parse_response.failed_calls = 0
                    initialize.side_effect = initialize_mock
                    mocks["can_read"].return_value = False
                    mocks["send_command"].return_value = "MOCK_OK"
                    mocks["connect"].return_value = None
                    with patch.object(
                        CommandsParser, "initialize", autospec=True
                    ) as cmd_parser_initialize:

                        def cmd_init_mock(self, r):
                            self.commands = {
                                "get": {
                                    "name": "get",
                                    "arity": 2,
                                    "flags": ["readonly", "fast"],
                                    "first_key_pos": 1,
                                    "last_key_pos": 1,
                                    "step_count": 1,
                                }
                            }

                        cmd_parser_initialize.side_effect = cmd_init_mock

                        rc = _get_client(RedisCluster, request, flushdb=False)
                        assert len(rc.get_nodes()) == 1
                        assert rc.get_node(node_name=node_7006.name) is not None

                        rc.get("foo")

                        # Cluster should now point to 7007, and there should be
                        # one failed and one successful call
                        assert len(rc.get_nodes()) == 1
                        assert rc.get_node(node_name=node_7007.name) is not None
                        assert rc.get_node(node_name=node_7006.name) is None
                        assert parse_response.failed_calls == 1
                        assert parse_response.successful_calls == 1

    def test_reading_from_replicas_in_round_robin(self):
        with patch.multiple(
            Connection,
            send_command=DEFAULT,
            read_response=DEFAULT,
            _connect=DEFAULT,
            can_read=DEFAULT,
            on_connect=DEFAULT,
        ) as mocks:
            with patch.object(Redis, "parse_response") as parse_response:

                def parse_response_mock_first(connection, *args, **options):
                    # Primary
                    assert connection.port == 7001
                    parse_response.side_effect = parse_response_mock_second
                    return "MOCK_OK"

                def parse_response_mock_second(connection, *args, **options):
                    # Replica
                    assert connection.port == 7002
                    parse_response.side_effect = parse_response_mock_third
                    return "MOCK_OK"

                def parse_response_mock_third(connection, *args, **options):
                    # Primary
                    assert connection.port == 7001
                    return "MOCK_OK"

                # We don't need to create a real cluster connection but we
                # do want RedisCluster.on_connect function to get called,
                # so we'll mock some of the Connection's functions to allow it
                parse_response.side_effect = parse_response_mock_first
                mocks["send_command"].return_value = True
                mocks["read_response"].return_value = "OK"
                mocks["_connect"].return_value = True
                mocks["can_read"].return_value = False
                mocks["on_connect"].return_value = True

                # Create a cluster with reading from replications
                read_cluster = get_mocked_redis_client(
                    host=default_host, port=default_port, read_from_replicas=True
                )
                assert read_cluster.read_from_replicas is True
                # Check that we read from the slot's nodes in a round robin
                # matter.
                # 'foo' belongs to slot 12182 and the slot's nodes are:
                # [(127.0.0.1,7001,primary), (127.0.0.1,7002,replica)]
                read_cluster.get("foo")
                read_cluster.get("foo")
                read_cluster.get("foo")
                mocks["send_command"].assert_has_calls(
                    [
                        call("READONLY"),
                        call("GET", "foo", keys=["foo"]),
                        call("READONLY"),
                        call("GET", "foo", keys=["foo"]),
                        call("GET", "foo", keys=["foo"]),
                    ]
                )

    def test_keyslot(self, r):
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

    def test_get_node_name(self):
        assert (
            get_node_name(default_host, default_port)
            == f"{default_host}:{default_port}"
        )

    def test_all_nodes(self, r):
        """
        Set a list of nodes and it should be possible to iterate over all
        """
        nodes = [node for node in r.nodes_manager.nodes_cache.values()]

        for i, node in enumerate(r.get_nodes()):
            assert node in nodes

    def test_all_nodes_masters(self, r):
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
    def test_cluster_down_overreaches_retry_attempts(self, error):
        """
        When error that allows retry is thrown, test that we retry executing
        the command as many times as configured in cluster_error_retry_attempts
        and then raise the exception
        """
        with patch.object(RedisCluster, "_execute_command") as execute_command:

            def raise_error(target_node, *args, **kwargs):
                execute_command.failed_calls += 1
                raise error("mocked error")

            execute_command.side_effect = raise_error

            rc = get_mocked_redis_client(host=default_host, port=default_port)

            with pytest.raises(error):
                rc.get("bar")
                assert execute_command.failed_calls == rc.cluster_error_retry_attempts

    def test_user_on_connect_function(self, request):
        """
        Test support in passing on_connect function by the user
        """

        def on_connect(connection):
            assert connection is not None

        mock = Mock(side_effect=on_connect)

        _get_client(RedisCluster, request, redis_connect_func=mock)
        assert mock.called is True

    def test_set_default_node_success(self, r):
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
        assert r.set_default_node(new_def_node) is True
        assert r.get_default_node() == new_def_node

    def test_set_default_node_failure(self, r):
        """
        test failed replacement of the default cluster node
        """
        default_node = r.get_default_node()
        new_def_node = ClusterNode("1.1.1.1", 1111)
        assert r.set_default_node(None) is False
        assert r.set_default_node(new_def_node) is False
        assert r.get_default_node() == default_node

    def test_get_node_from_key(self, r):
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
    def test_not_require_full_coverage_cluster_down_error(self, r):
        """
        When require_full_coverage is set to False (default client config) and not
        all slots are covered, if one of the nodes has 'cluster-require_full_coverage'
        config set to 'yes' some key-based commands should throw ClusterDownError
        """
        node = r.get_node_from_key("foo")
        missing_slot = r.keyslot("foo")
        assert r.set("foo", "bar") is True
        try:
            assert all(r.cluster_delslots(missing_slot))
            with pytest.raises(ClusterDownError):
                r.exists("foo")
        except ResponseError as e:
            assert "CLUSTERDOWN" in str(e)
        finally:
            try:
                # Add back the missing slot
                assert r.cluster_addslots(node, missing_slot) is True
                # Make sure we are not getting ClusterDownError anymore
                assert r.exists("foo") == 1
            except ResponseError as e:
                if f"Slot {missing_slot} is already busy" in str(e):
                    # It can happen if the test failed to delete this slot
                    pass
                else:
                    raise e

    def test_timeout_error_topology_refresh_reuse_connections(self, r):
        """
        By mucking TIMEOUT errors, we'll force the cluster topology to be reinitialized,
        and then ensure that only the impacted connection is replaced
        """
        node = r.get_node_from_key("key")
        r.set("key", "value")
        node_conn_origin = {}
        for n in r.get_nodes():
            node_conn_origin[n.name] = n.redis_connection
        real_func = r.get_redis_connection(node).parse_response

        class counter:
            def __init__(self, val=0):
                self.val = int(val)

        count = counter(0)
        with patch.object(Redis, "parse_response") as parse_response:

            def moved_redirect_effect(connection, *args, **options):
                # raise a timeout for 5 times so we'll need to reinitialize the topology
                if count.val == 4:
                    parse_response.side_effect = real_func
                count.val += 1
                raise TimeoutError()

            parse_response.side_effect = moved_redirect_effect
            assert r.get("key") == b"value"
            for node_name, conn in node_conn_origin.items():
                if node_name == node.name:
                    # The old redis connection of the timed out node should have been
                    # deleted and replaced
                    assert conn != r.get_redis_connection(node)
                else:
                    # other nodes' redis connection should have been reused during the
                    # topology refresh
                    cur_node = r.get_node(node_name=node_name)
                    assert conn == r.get_redis_connection(cur_node)

    def test_cluster_get_set_retry_object(self, request):
        retry = Retry(NoBackoff(), 2)
        r = _get_client(RedisCluster, request, retry=retry)
        assert r.get_retry()._retries == retry._retries
        assert isinstance(r.get_retry()._backoff, NoBackoff)
        for node in r.get_nodes():
            assert node.redis_connection.get_retry()._retries == retry._retries
            assert isinstance(node.redis_connection.get_retry()._backoff, NoBackoff)
        rand_node = r.get_random_node()
        existing_conn = rand_node.redis_connection.connection_pool.get_connection("_")
        # Change retry policy
        new_retry = Retry(ExponentialBackoff(), 3)
        r.set_retry(new_retry)
        assert r.get_retry()._retries == new_retry._retries
        assert isinstance(r.get_retry()._backoff, ExponentialBackoff)
        for node in r.get_nodes():
            assert node.redis_connection.get_retry()._retries == new_retry._retries
            assert isinstance(
                node.redis_connection.get_retry()._backoff, ExponentialBackoff
            )
        assert existing_conn.retry._retries == new_retry._retries
        new_conn = rand_node.redis_connection.connection_pool.get_connection("_")
        assert new_conn.retry._retries == new_retry._retries

    def test_cluster_retry_object(self, r) -> None:
        # Test default retry
        retry = r.get_connection_kwargs().get("retry")
        assert isinstance(retry, Retry)
        assert retry._retries == 0
        assert isinstance(retry._backoff, type(default_backoff()))
        node1 = r.get_node("127.0.0.1", 16379).redis_connection
        node2 = r.get_node("127.0.0.1", 16380).redis_connection
        assert node1.get_retry()._retries == node2.get_retry()._retries

        # Test custom retry
        retry = Retry(ExponentialBackoff(10, 5), 5)
        rc_custom_retry = RedisCluster("127.0.0.1", 16379, retry=retry)
        assert (
            rc_custom_retry.get_node("127.0.0.1", 16379)
            .redis_connection.get_retry()
            ._retries
            == retry._retries
        )

    def test_replace_cluster_node(self, r) -> None:
        prev_default_node = r.get_default_node()
        r.replace_default_node()
        assert r.get_default_node() != prev_default_node
        r.replace_default_node(prev_default_node)
        assert r.get_default_node() == prev_default_node

    def test_default_node_is_replaced_after_exception(self, r):
        curr_default_node = r.get_default_node()
        # CLUSTER NODES command is being executed on the default node
        nodes = r.cluster_nodes()
        assert "myself" in nodes.get(curr_default_node.name).get("flags")

        def raise_connection_error():
            raise ConnectionError("error")

        # Mock connection error for the default node
        mock_node_resp_func(curr_default_node, raise_connection_error)
        # Test that the command succeed from a different node
        nodes = r.cluster_nodes()
        assert "myself" not in nodes.get(curr_default_node.name).get("flags")
        assert r.get_default_node() != curr_default_node

    def test_address_remap(self, request, master_host):
        """Test that we can create a rediscluster object with
        a host-port remapper and map connections through proxy objects
        """

        # we remap the first n nodes
        offset = 1000
        n = 6
        hostname, master_port = master_host
        ports = [master_port + i for i in range(n)]

        def address_remap(address):
            # remap first three nodes to our local proxy
            # old = host, port
            host, port = address
            if int(port) in ports:
                host, port = "127.0.0.1", int(port) + offset
            # print(f"{old} {host, port}")
            return host, port

        # create the proxies
        proxies = [
            NodeProxy(("127.0.0.1", port + offset), (hostname, port)) for port in ports
        ]
        for p in proxies:
            p.start()
        try:
            # create cluster:
            r = _get_client(
                RedisCluster, request, flushdb=False, address_remap=address_remap
            )
            try:
                assert r.ping() is True
                assert r.set("byte_string", b"giraffe")
                assert r.get("byte_string") == b"giraffe"
            finally:
                r.close()
        finally:
            for p in proxies:
                p.close()

        # verify that the proxies were indeed used
        n_used = sum((1 if p.n_connections else 0) for p in proxies)
        assert n_used > 1


@pytest.mark.onlycluster
class TestClusterRedisCommands:
    """
    Tests for RedisCluster unique commands
    """

    def test_case_insensitive_command_names(self, r):
        assert (
            r.cluster_response_callbacks["cluster slots"]
            == r.cluster_response_callbacks["CLUSTER SLOTS"]
        )

    def test_get_and_set(self, r):
        # get and set can't be tested independently of each other
        assert r.get("a") is None
        byte_string = b"value"
        integer = 5
        unicode_string = chr(3456) + "abcd" + chr(3421)
        assert r.set("byte_string", byte_string)
        assert r.set("integer", 5)
        assert r.set("unicode_string", unicode_string)
        assert r.get("byte_string") == byte_string
        assert r.get("integer") == str(integer).encode()
        assert r.get("unicode_string").decode("utf-8") == unicode_string

    def test_mget_nonatomic(self, r):
        assert r.mget_nonatomic([]) == []
        assert r.mget_nonatomic(["a", "b"]) == [None, None]
        r["a"] = "1"
        r["b"] = "2"
        r["c"] = "3"

        assert r.mget_nonatomic("a", "other", "b", "c") == [b"1", None, b"2", b"3"]

    def test_mset_nonatomic(self, r):
        d = {"a": b"1", "b": b"2", "c": b"3", "d": b"4"}
        assert r.mset_nonatomic(d)
        for k, v in d.items():
            assert r[k] == v

    def test_config_set(self, r):
        assert r.config_set("slowlog-log-slower-than", 0)

    def test_cluster_config_resetstat(self, r):
        r.ping(target_nodes="all")
        all_info = r.info(target_nodes="all")
        prior_commands_processed = -1
        for node_info in all_info.values():
            prior_commands_processed = node_info["total_commands_processed"]
            assert prior_commands_processed >= 1
        r.config_resetstat(target_nodes="all")
        all_info = r.info(target_nodes="all")
        for node_info in all_info.values():
            reset_commands_processed = node_info["total_commands_processed"]
            assert reset_commands_processed < prior_commands_processed

    def test_client_setname(self, r):
        node = r.get_random_node()
        r.client_setname("redis_py_test", target_nodes=node)
        client_name = r.client_getname(target_nodes=node)
        assert_resp_response(r, client_name, "redis_py_test", b"redis_py_test")

    def test_exists(self, r):
        d = {"a": b"1", "b": b"2", "c": b"3", "d": b"4"}
        r.mset_nonatomic(d)
        assert r.exists(*d.keys()) == len(d)

    def test_delete(self, r):
        d = {"a": b"1", "b": b"2", "c": b"3", "d": b"4"}
        r.mset_nonatomic(d)
        assert r.delete(*d.keys()) == len(d)
        assert r.delete(*d.keys()) == 0

    def test_touch(self, r):
        d = {"a": b"1", "b": b"2", "c": b"3", "d": b"4"}
        r.mset_nonatomic(d)
        assert r.touch(*d.keys()) == len(d)

    def test_unlink(self, r):
        d = {"a": b"1", "b": b"2", "c": b"3", "d": b"4"}
        r.mset_nonatomic(d)
        assert r.unlink(*d.keys()) == len(d)
        # Unlink is non-blocking so we sleep before
        # verifying the deletion
        sleep(0.1)
        assert r.unlink(*d.keys()) == 0

    def test_pubsub_channels_merge_results(self, r):
        nodes = r.get_nodes()
        channels = []
        pubsub_nodes = []
        i = 0
        for node in nodes:
            channel = f"foo{i}"
            # We will create different pubsub clients where each one is
            # connected to a different node
            p = r.pubsub(node)
            pubsub_nodes.append(p)
            p.subscribe(channel)
            b_channel = channel.encode("utf-8")
            channels.append(b_channel)
            # Assert that each node returns only the channel it subscribed to
            sub_channels = node.redis_connection.pubsub_channels()
            if not sub_channels:
                # Try again after a short sleep
                sleep(0.3)
                sub_channels = node.redis_connection.pubsub_channels()
            assert sub_channels == [b_channel]
            i += 1
        # Assert that the cluster's pubsub_channels function returns ALL of
        # the cluster's channels
        result = r.pubsub_channels(target_nodes="all")
        result.sort()
        assert result == channels

    def test_pubsub_numsub_merge_results(self, r):
        nodes = r.get_nodes()
        pubsub_nodes = []
        channel = "foo"
        b_channel = channel.encode("utf-8")
        for node in nodes:
            # We will create different pubsub clients where each one is
            # connected to a different node
            p = r.pubsub(node)
            pubsub_nodes.append(p)
            p.subscribe(channel)
            # Assert that each node returns that only one client is subscribed
            sub_chann_num = node.redis_connection.pubsub_numsub(channel)
            if sub_chann_num == [(b_channel, 0)]:
                sleep(0.3)
                sub_chann_num = node.redis_connection.pubsub_numsub(channel)
            assert sub_chann_num == [(b_channel, 1)]
        # Assert that the cluster's pubsub_numsub function returns ALL clients
        # subscribed to this channel in the entire cluster
        assert r.pubsub_numsub(channel, target_nodes="all") == [(b_channel, len(nodes))]

    def test_pubsub_numpat_merge_results(self, r):
        nodes = r.get_nodes()
        pubsub_nodes = []
        pattern = "foo*"
        for node in nodes:
            # We will create different pubsub clients where each one is
            # connected to a different node
            p = r.pubsub(node)
            pubsub_nodes.append(p)
            p.psubscribe(pattern)
            # Assert that each node returns that only one client is subscribed
            sub_num_pat = node.redis_connection.pubsub_numpat()
            if sub_num_pat == 0:
                sleep(0.3)
                sub_num_pat = node.redis_connection.pubsub_numpat()
            assert sub_num_pat == 1
        # Assert that the cluster's pubsub_numsub function returns ALL clients
        # subscribed to this channel in the entire cluster
        assert r.pubsub_numpat(target_nodes="all") == len(nodes)

    @skip_if_server_version_lt("2.8.0")
    def test_cluster_pubsub_channels(self, r):
        p = r.pubsub()
        p.subscribe("foo", "bar", "baz", "quux")
        for i in range(4):
            assert wait_for_message(p, timeout=0.5)["type"] == "subscribe"
        expected = [b"bar", b"baz", b"foo", b"quux"]
        assert all(
            [channel in r.pubsub_channels(target_nodes="all") for channel in expected]
        )

    @skip_if_server_version_lt("2.8.0")
    def test_cluster_pubsub_numsub(self, r):
        p1 = r.pubsub()
        p1.subscribe("foo", "bar", "baz")
        for i in range(3):
            assert wait_for_message(p1, timeout=0.5)["type"] == "subscribe"
        p2 = r.pubsub()
        p2.subscribe("bar", "baz")
        for i in range(2):
            assert wait_for_message(p2, timeout=0.5)["type"] == "subscribe"
        p3 = r.pubsub()
        p3.subscribe("baz")
        assert wait_for_message(p3, timeout=0.5)["type"] == "subscribe"

        channels = [(b"foo", 1), (b"bar", 2), (b"baz", 3)]
        assert r.pubsub_numsub("foo", "bar", "baz", target_nodes="all") == channels

    @skip_if_redis_enterprise()
    def test_cluster_myid(self, r):
        node = r.get_random_node()
        myid = r.cluster_myid(node)
        assert len(myid) == 40

    @skip_if_redis_enterprise()
    def test_cluster_slots(self, r):
        mock_all_nodes_resp(r, default_cluster_slots)
        cluster_slots = r.cluster_slots()
        assert isinstance(cluster_slots, dict)
        assert len(default_cluster_slots) == len(cluster_slots)
        assert cluster_slots.get((0, 8191)) is not None
        assert cluster_slots.get((0, 8191)).get("primary") == ("127.0.0.1", 7000)

    @skip_if_server_version_lt("7.0.0")
    @skip_if_redis_enterprise()
    def test_cluster_shards(self, r):
        cluster_shards = r.cluster_shards()
        assert isinstance(cluster_shards, list)
        assert isinstance(cluster_shards[0], dict)
        attributes = [
            b"id",
            b"endpoint",
            b"ip",
            b"hostname",
            b"port",
            b"tls-port",
            b"role",
            b"replication-offset",
            b"health",
        ]
        for x in cluster_shards:
            assert_resp_response(
                r, list(x.keys()), ["slots", "nodes"], [b"slots", b"nodes"]
            )
            try:
                x["nodes"]
                key = "nodes"
            except KeyError:
                key = b"nodes"
            for node in x[key]:
                for attribute in node.keys():
                    assert attribute in attributes

    @skip_if_server_version_lt("7.2.0")
    @skip_if_redis_enterprise()
    def test_cluster_myshardid(self, r):
        myshardid = r.cluster_myshardid()
        assert isinstance(myshardid, str)
        assert len(myshardid) > 0

    @skip_if_redis_enterprise()
    def test_cluster_addslots(self, r):
        node = r.get_random_node()
        mock_node_resp(node, "OK")
        assert r.cluster_addslots(node, 1, 2, 3) is True

    @skip_if_server_version_lt("7.0.0")
    @skip_if_redis_enterprise()
    def test_cluster_addslotsrange(self, r):
        node = r.get_random_node()
        mock_node_resp(node, "OK")
        assert r.cluster_addslotsrange(node, 1, 5)

    @skip_if_redis_enterprise()
    def test_cluster_countkeysinslot(self, r):
        node = r.nodes_manager.get_node_from_slot(1)
        mock_node_resp(node, 2)
        assert r.cluster_countkeysinslot(1) == 2

    def test_cluster_count_failure_report(self, r):
        mock_all_nodes_resp(r, 0)
        assert r.cluster_count_failure_report("node_0") == 0

    @skip_if_redis_enterprise()
    def test_cluster_delslots(self):
        cluster_slots = [
            [0, 8191, ["127.0.0.1", 7000, "node_0"]],
            [8192, 16383, ["127.0.0.1", 7001, "node_1"]],
        ]
        r = get_mocked_redis_client(
            host=default_host, port=default_port, cluster_slots=cluster_slots
        )
        mock_all_nodes_resp(r, "OK")
        node0 = r.get_node(default_host, 7000)
        node1 = r.get_node(default_host, 7001)
        assert r.cluster_delslots(0, 8192) == [True, True]
        assert node0.redis_connection.connection.read_response.called
        assert node1.redis_connection.connection.read_response.called

    @skip_if_server_version_lt("7.0.0")
    @skip_if_redis_enterprise()
    def test_cluster_delslotsrange(self):
        cluster_slots = [
            [
                0,
                8191,
                ["127.0.0.1", 7000, "node_0"],
            ],
            [
                8192,
                16383,
                ["127.0.0.1", 7001, "node_1"],
            ],
        ]
        r = get_mocked_redis_client(
            host=default_host, port=default_port, cluster_slots=cluster_slots
        )
        mock_all_nodes_resp(r, "OK")
        node = r.get_random_node()
        r.cluster_addslots(node, 1, 2, 3, 4, 5)
        assert r.cluster_delslotsrange(1, 5)

    @skip_if_redis_enterprise()
    def test_cluster_failover(self, r):
        node = r.get_random_node()
        mock_node_resp(node, "OK")
        assert r.cluster_failover(node) is True
        assert r.cluster_failover(node, "FORCE") is True
        assert r.cluster_failover(node, "TAKEOVER") is True
        with pytest.raises(RedisError):
            r.cluster_failover(node, "FORCT")

    @skip_if_redis_enterprise()
    def test_cluster_info(self, r):
        info = r.cluster_info()
        assert isinstance(info, dict)
        assert info["cluster_state"] == "ok"

    @skip_if_redis_enterprise()
    def test_cluster_keyslot(self, r):
        mock_all_nodes_resp(r, 12182)
        assert r.cluster_keyslot("foo") == 12182

    @skip_if_redis_enterprise()
    def test_cluster_meet(self, r):
        node = r.get_default_node()
        mock_node_resp(node, "OK")
        assert r.cluster_meet("127.0.0.1", 6379) is True

    @skip_if_redis_enterprise()
    def test_cluster_nodes(self, r):
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
        nodes = r.cluster_nodes()
        assert len(nodes) == 7
        assert nodes.get("172.17.0.7:7006") is not None
        assert (
            nodes.get("172.17.0.7:7006").get("node_id")
            == "c8253bae761cb1ecb2b61857d85dfe455a0fec8b"
        )

    @skip_if_redis_enterprise()
    def test_cluster_nodes_importing_migrating(self, r):
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
        nodes = r.cluster_nodes()
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
    def test_cluster_replicate(self, r):
        node = r.get_random_node()
        all_replicas = r.get_replicas()
        mock_all_nodes_resp(r, "OK")
        assert r.cluster_replicate(node, "c8253bae761cb61857d") is True
        results = r.cluster_replicate(all_replicas, "c8253bae761cb61857d")
        if isinstance(results, dict):
            for res in results.values():
                assert res is True
        else:
            assert results is True

    @skip_if_redis_enterprise()
    def test_cluster_reset(self, r):
        mock_all_nodes_resp(r, "OK")
        assert r.cluster_reset() is True
        assert r.cluster_reset(False) is True
        all_results = r.cluster_reset(False, target_nodes="all")
        for res in all_results.values():
            assert res is True

    @skip_if_redis_enterprise()
    def test_cluster_save_config(self, r):
        node = r.get_random_node()
        all_nodes = r.get_nodes()
        mock_all_nodes_resp(r, "OK")
        assert r.cluster_save_config(node) is True
        all_results = r.cluster_save_config(all_nodes)
        for res in all_results.values():
            assert res is True

    @skip_if_redis_enterprise()
    def test_cluster_get_keys_in_slot(self, r):
        response = ["{foo}1", "{foo}2"]
        node = r.nodes_manager.get_node_from_slot(12182)
        mock_node_resp(node, response)
        keys = r.cluster_get_keys_in_slot(12182, 4)
        assert keys == response

    @skip_if_redis_enterprise()
    def test_cluster_set_config_epoch(self, r):
        mock_all_nodes_resp(r, "OK")
        assert r.cluster_set_config_epoch(3) is True
        all_results = r.cluster_set_config_epoch(3, target_nodes="all")
        for res in all_results.values():
            assert res is True

    @skip_if_redis_enterprise()
    def test_cluster_setslot(self, r):
        node = r.get_random_node()
        mock_node_resp(node, "OK")
        assert r.cluster_setslot(node, "node_0", 1218, "IMPORTING") is True
        assert r.cluster_setslot(node, "node_0", 1218, "NODE") is True
        assert r.cluster_setslot(node, "node_0", 1218, "MIGRATING") is True
        with pytest.raises(RedisError):
            r.cluster_failover(node, "STABLE")
        with pytest.raises(RedisError):
            r.cluster_failover(node, "STATE")

    def test_cluster_setslot_stable(self, r):
        node = r.nodes_manager.get_node_from_slot(12182)
        mock_node_resp(node, "OK")
        assert r.cluster_setslot_stable(12182) is True
        assert node.redis_connection.connection.read_response.called

    @skip_if_redis_enterprise()
    def test_cluster_replicas(self, r):
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
        replicas = r.cluster_replicas("52611e796814b78e90ad94be9d769a4f668f9a")
        assert replicas.get("127.0.0.1:6377") is not None
        assert replicas.get("127.0.0.1:6378") is not None
        assert (
            replicas.get("127.0.0.1:6378").get("node_id")
            == "r4xfga22229cf3c652b6fca0d09ff69f3e0d4d"
        )

    @skip_if_server_version_lt("7.0.0")
    def test_cluster_links(self, r):
        node = r.get_random_node()
        res = r.cluster_links(node)
        if is_resp2_connection(r):
            links_to = sum(x.count(b"to") for x in res)
            links_for = sum(x.count(b"from") for x in res)
            assert links_to == links_for
            for i in range(0, len(res) - 1, 2):
                assert res[i][3] == res[i + 1][3]
        else:
            links_to = len(list(filter(lambda x: x[b"direction"] == b"to", res)))
            links_for = len(list(filter(lambda x: x[b"direction"] == b"from", res)))
            assert links_to == links_for
            for i in range(0, len(res) - 1, 2):
                assert res[i][b"node"] == res[i + 1][b"node"]

    def test_cluster_flshslots_not_implemented(self, r):
        with pytest.raises(NotImplementedError):
            r.cluster_flushslots()

    def test_cluster_bumpepoch_not_implemented(self, r):
        with pytest.raises(NotImplementedError):
            r.cluster_bumpepoch()

    @skip_if_redis_enterprise()
    def test_readonly(self):
        r = get_mocked_redis_client(host=default_host, port=default_port)
        mock_all_nodes_resp(r, "OK")
        assert r.readonly() is True
        all_replicas_results = r.readonly(target_nodes="replicas")
        for res in all_replicas_results.values():
            assert res is True
        for replica in r.get_replicas():
            assert replica.redis_connection.connection.read_response.called

    @skip_if_redis_enterprise()
    def test_readwrite(self):
        r = get_mocked_redis_client(host=default_host, port=default_port)
        mock_all_nodes_resp(r, "OK")
        assert r.readwrite() is True
        all_replicas_results = r.readwrite(target_nodes="replicas")
        for res in all_replicas_results.values():
            assert res is True
        for replica in r.get_replicas():
            assert replica.redis_connection.connection.read_response.called

    @skip_if_redis_enterprise()
    def test_bgsave(self, r):
        assert r.bgsave()
        sleep(0.3)
        assert r.bgsave(True)

    def test_info(self, r):
        # Map keys to same slot
        r.set("x{1}", 1)
        r.set("y{1}", 2)
        r.set("z{1}", 3)
        # Get node that handles the slot
        slot = r.keyslot("x{1}")
        node = r.nodes_manager.get_node_from_slot(slot)
        # Run info on that node
        info = r.info(target_nodes=node)
        assert isinstance(info, dict)
        assert info["db0"]["keys"] == 3

    def _init_slowlog_test(self, r, node):
        slowlog_lim = r.config_get("slowlog-log-slower-than", target_nodes=node)
        assert r.config_set("slowlog-log-slower-than", 0, target_nodes=node) is True
        return slowlog_lim["slowlog-log-slower-than"]

    def _teardown_slowlog_test(self, r, node, prev_limit):
        assert (
            r.config_set("slowlog-log-slower-than", prev_limit, target_nodes=node)
            is True
        )

    def test_slowlog_get(self, r, slowlog):
        unicode_string = chr(3456) + "abcd" + chr(3421)
        node = r.get_node_from_key(unicode_string)
        slowlog_limit = self._init_slowlog_test(r, node)
        assert r.slowlog_reset(target_nodes=node)
        r.get(unicode_string)
        slowlog = r.slowlog_get(target_nodes=node)
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
        self._teardown_slowlog_test(r, node, slowlog_limit)

    def test_slowlog_get_limit(self, r, slowlog):
        assert r.slowlog_reset()
        node = r.get_node_from_key("foo")
        slowlog_limit = self._init_slowlog_test(r, node)
        r.get("foo")
        slowlog = r.slowlog_get(1, target_nodes=node)
        assert isinstance(slowlog, list)
        # only one command, based on the number we passed to slowlog_get()
        assert len(slowlog) == 1
        self._teardown_slowlog_test(r, node, slowlog_limit)

    def test_slowlog_length(self, r, slowlog):
        r.get("foo")
        node = r.nodes_manager.get_node_from_slot(key_slot(b"foo"))
        slowlog_len = r.slowlog_len(target_nodes=node)
        assert isinstance(slowlog_len, int)

    def test_time(self, r):
        t = r.time(target_nodes=r.get_primaries()[0])
        assert len(t) == 2
        assert isinstance(t[0], int)
        assert isinstance(t[1], int)

    @skip_if_server_version_lt("4.0.0")
    def test_memory_usage(self, r):
        r.set("foo", "bar")
        assert isinstance(r.memory_usage("foo"), int)

    @skip_if_server_version_lt("4.0.0")
    @skip_if_redis_enterprise()
    def test_memory_malloc_stats(self, r):
        assert r.memory_malloc_stats()

    @skip_if_server_version_lt("4.0.0")
    @skip_if_redis_enterprise()
    def test_memory_stats(self, r):
        # put a key into the current db to make sure that "db.<current-db>"
        # has data
        r.set("foo", "bar")
        node = r.nodes_manager.get_node_from_slot(key_slot(b"foo"))
        stats = r.memory_stats(target_nodes=node)
        assert isinstance(stats, dict)
        for key, value in stats.items():
            if key.startswith("db."):
                assert not isinstance(value, list)

    @skip_if_server_version_lt("4.0.0")
    def test_memory_help(self, r):
        with pytest.raises(NotImplementedError):
            r.memory_help()

    @skip_if_server_version_lt("4.0.0")
    def test_memory_doctor(self, r):
        with pytest.raises(NotImplementedError):
            r.memory_doctor()

    @skip_if_redis_enterprise()
    def test_lastsave(self, r):
        node = r.get_primaries()[0]
        assert isinstance(r.lastsave(target_nodes=node), datetime.datetime)

    def test_cluster_echo(self, r):
        node = r.get_primaries()[0]
        assert r.echo("foo bar", target_nodes=node) == b"foo bar"

    @skip_if_server_version_lt("1.0.0")
    def test_debug_segfault(self, r):
        with pytest.raises(NotImplementedError):
            r.debug_segfault()

    def test_config_resetstat(self, r):
        node = r.get_primaries()[0]
        r.ping(target_nodes=node)
        prior_commands_processed = int(
            r.info(target_nodes=node)["total_commands_processed"]
        )
        assert prior_commands_processed >= 1
        r.config_resetstat(target_nodes=node)
        reset_commands_processed = int(
            r.info(target_nodes=node)["total_commands_processed"]
        )
        assert reset_commands_processed < prior_commands_processed

    @skip_if_server_version_lt("6.2.0")
    def test_client_trackinginfo(self, r):
        node = r.get_primaries()[0]
        res = r.client_trackinginfo(target_nodes=node)
        assert len(res) > 2
        assert "prefixes" in res or b"prefixes" in res

    @skip_if_server_version_lt("2.9.50")
    def test_client_pause(self, r):
        node = r.get_primaries()[0]
        assert r.client_pause(1, target_nodes=node)
        assert r.client_pause(timeout=1, target_nodes=node)
        with pytest.raises(RedisError):
            r.client_pause(timeout="not an integer", target_nodes=node)

    @skip_if_server_version_lt("6.2.0")
    @skip_if_redis_enterprise()
    def test_client_unpause(self, r):
        assert r.client_unpause()

    @skip_if_server_version_lt("5.0.0")
    def test_client_id(self, r):
        node = r.get_primaries()[0]
        assert r.client_id(target_nodes=node) > 0

    @skip_if_server_version_lt("5.0.0")
    def test_client_unblock(self, r):
        node = r.get_primaries()[0]
        myid = r.client_id(target_nodes=node)
        assert not r.client_unblock(myid, target_nodes=node)
        assert not r.client_unblock(myid, error=True, target_nodes=node)
        assert not r.client_unblock(myid, error=False, target_nodes=node)

    @skip_if_server_version_lt("6.0.0")
    def test_client_getredir(self, r):
        node = r.get_primaries()[0]
        assert isinstance(r.client_getredir(target_nodes=node), int)
        assert r.client_getredir(target_nodes=node) == -1

    @skip_if_server_version_lt("6.2.0")
    def test_client_info(self, r):
        node = r.get_primaries()[0]
        info = r.client_info(target_nodes=node)
        assert isinstance(info, dict)
        assert "addr" in info

    @skip_if_server_version_lt("2.6.9")
    def test_client_kill(self, r, r2):
        node = r.get_primaries()[0]
        r.client_setname("redis-py-c1", target_nodes="all")
        r2.client_setname("redis-py-c2", target_nodes="all")
        clients = [
            client
            for client in r.client_list(target_nodes=node)
            if client.get("name") in ["redis-py-c1", "redis-py-c2"]
        ]
        assert len(clients) == 2
        clients_by_name = {client.get("name"): client for client in clients}

        client_addr = clients_by_name["redis-py-c2"].get("addr")
        assert r.client_kill(client_addr, target_nodes=node) is True

        clients = [
            client
            for client in r.client_list(target_nodes=node)
            if client.get("name") in ["redis-py-c1", "redis-py-c2"]
        ]
        assert len(clients) == 1
        assert clients[0].get("name") == "redis-py-c1"

    @skip_if_server_version_lt("2.6.0")
    def test_cluster_bitop_not_empty_string(self, r):
        r["{foo}a"] = ""
        r.bitop("not", "{foo}r", "{foo}a")
        assert r.get("{foo}r") is None

    @skip_if_server_version_lt("2.6.0")
    def test_cluster_bitop_not(self, r):
        test_str = b"\xAA\x00\xFF\x55"
        correct = ~0xAA00FF55 & 0xFFFFFFFF
        r["{foo}a"] = test_str
        r.bitop("not", "{foo}r", "{foo}a")
        assert int(binascii.hexlify(r["{foo}r"]), 16) == correct

    @skip_if_server_version_lt("2.6.0")
    def test_cluster_bitop_not_in_place(self, r):
        test_str = b"\xAA\x00\xFF\x55"
        correct = ~0xAA00FF55 & 0xFFFFFFFF
        r["{foo}a"] = test_str
        r.bitop("not", "{foo}a", "{foo}a")
        assert int(binascii.hexlify(r["{foo}a"]), 16) == correct

    @skip_if_server_version_lt("2.6.0")
    def test_cluster_bitop_single_string(self, r):
        test_str = b"\x01\x02\xFF"
        r["{foo}a"] = test_str
        r.bitop("and", "{foo}res1", "{foo}a")
        r.bitop("or", "{foo}res2", "{foo}a")
        r.bitop("xor", "{foo}res3", "{foo}a")
        assert r["{foo}res1"] == test_str
        assert r["{foo}res2"] == test_str
        assert r["{foo}res3"] == test_str

    @skip_if_server_version_lt("2.6.0")
    def test_cluster_bitop_string_operands(self, r):
        r["{foo}a"] = b"\x01\x02\xFF\xFF"
        r["{foo}b"] = b"\x01\x02\xFF"
        r.bitop("and", "{foo}res1", "{foo}a", "{foo}b")
        r.bitop("or", "{foo}res2", "{foo}a", "{foo}b")
        r.bitop("xor", "{foo}res3", "{foo}a", "{foo}b")
        assert int(binascii.hexlify(r["{foo}res1"]), 16) == 0x0102FF00
        assert int(binascii.hexlify(r["{foo}res2"]), 16) == 0x0102FFFF
        assert int(binascii.hexlify(r["{foo}res3"]), 16) == 0x000000FF

    @skip_if_server_version_lt("6.2.0")
    def test_cluster_copy(self, r):
        assert r.copy("{foo}a", "{foo}b") == 0
        r.set("{foo}a", "bar")
        assert r.copy("{foo}a", "{foo}b") == 1
        assert r.get("{foo}a") == b"bar"
        assert r.get("{foo}b") == b"bar"

    @skip_if_server_version_lt("6.2.0")
    def test_cluster_copy_and_replace(self, r):
        r.set("{foo}a", "foo1")
        r.set("{foo}b", "foo2")
        assert r.copy("{foo}a", "{foo}b") == 0
        assert r.copy("{foo}a", "{foo}b", replace=True) == 1

    @skip_if_server_version_lt("6.2.0")
    def test_cluster_lmove(self, r):
        r.rpush("{foo}a", "one", "two", "three", "four")
        assert r.lmove("{foo}a", "{foo}b")
        assert r.lmove("{foo}a", "{foo}b", "right", "left")

    @skip_if_server_version_lt("6.2.0")
    def test_cluster_blmove(self, r):
        r.rpush("{foo}a", "one", "two", "three", "four")
        assert r.blmove("{foo}a", "{foo}b", 5)
        assert r.blmove("{foo}a", "{foo}b", 1, "RIGHT", "LEFT")

    def test_cluster_msetnx(self, r):
        d = {"{foo}a": b"1", "{foo}b": b"2", "{foo}c": b"3"}
        assert r.msetnx(d)
        d2 = {"{foo}a": b"x", "{foo}d": b"4"}
        assert not r.msetnx(d2)
        for k, v in d.items():
            assert r[k] == v
        assert r.get("{foo}d") is None

    def test_cluster_rename(self, r):
        r["{foo}a"] = "1"
        assert r.rename("{foo}a", "{foo}b")
        assert r.get("{foo}a") is None
        assert r["{foo}b"] == b"1"

    def test_cluster_renamenx(self, r):
        r["{foo}a"] = "1"
        r["{foo}b"] = "2"
        assert not r.renamenx("{foo}a", "{foo}b")
        assert r["{foo}a"] == b"1"
        assert r["{foo}b"] == b"2"

    # LIST COMMANDS
    def test_cluster_blpop(self, r):
        r.rpush("{foo}a", "1", "2")
        r.rpush("{foo}b", "3", "4")
        assert_resp_response(
            r,
            r.blpop(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}b", b"3"),
            [b"{foo}b", b"3"],
        )
        assert_resp_response(
            r,
            r.blpop(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}b", b"4"),
            [b"{foo}b", b"4"],
        )
        assert_resp_response(
            r,
            r.blpop(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}a", b"1"),
            [b"{foo}a", b"1"],
        )
        assert_resp_response(
            r,
            r.blpop(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}a", b"2"),
            [b"{foo}a", b"2"],
        )
        assert r.blpop(["{foo}b", "{foo}a"], timeout=1) is None
        r.rpush("{foo}c", "1")
        assert_resp_response(
            r, r.blpop("{foo}c", timeout=1), (b"{foo}c", b"1"), [b"{foo}c", b"1"]
        )

    def test_cluster_brpop(self, r):
        r.rpush("{foo}a", "1", "2")
        r.rpush("{foo}b", "3", "4")
        assert_resp_response(
            r,
            r.brpop(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}b", b"4"),
            [b"{foo}b", b"4"],
        )
        assert_resp_response(
            r,
            r.brpop(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}b", b"3"),
            [b"{foo}b", b"3"],
        )
        assert_resp_response(
            r,
            r.brpop(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}a", b"2"),
            [b"{foo}a", b"2"],
        )
        assert_resp_response(
            r,
            r.brpop(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}a", b"1"),
            [b"{foo}a", b"1"],
        )
        assert r.brpop(["{foo}b", "{foo}a"], timeout=1) is None
        r.rpush("{foo}c", "1")
        assert_resp_response(
            r, r.brpop("{foo}c", timeout=1), (b"{foo}c", b"1"), [b"{foo}c", b"1"]
        )

    def test_cluster_brpoplpush(self, r):
        r.rpush("{foo}a", "1", "2")
        r.rpush("{foo}b", "3", "4")
        assert r.brpoplpush("{foo}a", "{foo}b") == b"2"
        assert r.brpoplpush("{foo}a", "{foo}b") == b"1"
        assert r.brpoplpush("{foo}a", "{foo}b", timeout=1) is None
        assert r.lrange("{foo}a", 0, -1) == []
        assert r.lrange("{foo}b", 0, -1) == [b"1", b"2", b"3", b"4"]

    def test_cluster_brpoplpush_empty_string(self, r):
        r.rpush("{foo}a", "")
        assert r.brpoplpush("{foo}a", "{foo}b") == b""

    def test_cluster_rpoplpush(self, r):
        r.rpush("{foo}a", "a1", "a2", "a3")
        r.rpush("{foo}b", "b1", "b2", "b3")
        assert r.rpoplpush("{foo}a", "{foo}b") == b"a3"
        assert r.lrange("{foo}a", 0, -1) == [b"a1", b"a2"]
        assert r.lrange("{foo}b", 0, -1) == [b"a3", b"b1", b"b2", b"b3"]

    def test_cluster_sdiff(self, r):
        r.sadd("{foo}a", "1", "2", "3")
        assert r.sdiff("{foo}a", "{foo}b") == {b"1", b"2", b"3"}
        r.sadd("{foo}b", "2", "3")
        assert r.sdiff("{foo}a", "{foo}b") == {b"1"}

    def test_cluster_sdiffstore(self, r):
        r.sadd("{foo}a", "1", "2", "3")
        assert r.sdiffstore("{foo}c", "{foo}a", "{foo}b") == 3
        assert r.smembers("{foo}c") == {b"1", b"2", b"3"}
        r.sadd("{foo}b", "2", "3")
        assert r.sdiffstore("{foo}c", "{foo}a", "{foo}b") == 1
        assert r.smembers("{foo}c") == {b"1"}

    def test_cluster_sinter(self, r):
        r.sadd("{foo}a", "1", "2", "3")
        assert r.sinter("{foo}a", "{foo}b") == set()
        r.sadd("{foo}b", "2", "3")
        assert r.sinter("{foo}a", "{foo}b") == {b"2", b"3"}

    def test_cluster_sinterstore(self, r):
        r.sadd("{foo}a", "1", "2", "3")
        assert r.sinterstore("{foo}c", "{foo}a", "{foo}b") == 0
        assert r.smembers("{foo}c") == set()
        r.sadd("{foo}b", "2", "3")
        assert r.sinterstore("{foo}c", "{foo}a", "{foo}b") == 2
        assert r.smembers("{foo}c") == {b"2", b"3"}

    def test_cluster_smove(self, r):
        r.sadd("{foo}a", "a1", "a2")
        r.sadd("{foo}b", "b1", "b2")
        assert r.smove("{foo}a", "{foo}b", "a1")
        assert r.smembers("{foo}a") == {b"a2"}
        assert r.smembers("{foo}b") == {b"b1", b"b2", b"a1"}

    def test_cluster_sunion(self, r):
        r.sadd("{foo}a", "1", "2")
        r.sadd("{foo}b", "2", "3")
        assert r.sunion("{foo}a", "{foo}b") == {b"1", b"2", b"3"}

    def test_cluster_sunionstore(self, r):
        r.sadd("{foo}a", "1", "2")
        r.sadd("{foo}b", "2", "3")
        assert r.sunionstore("{foo}c", "{foo}a", "{foo}b") == 3
        assert r.smembers("{foo}c") == {b"1", b"2", b"3"}

    @skip_if_server_version_lt("6.2.0")
    def test_cluster_zdiff(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 2, "a3": 3})
        r.zadd("{foo}b", {"a1": 1, "a2": 2})
        assert r.zdiff(["{foo}a", "{foo}b"]) == [b"a3"]
        response = r.zdiff(["{foo}a", "{foo}b"], withscores=True)
        assert_resp_response(
            r,
            response,
            [b"a3", b"3"],
            [[b"a3", 3.0]],
        )

    @skip_if_server_version_lt("6.2.0")
    def test_cluster_zdiffstore(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 2, "a3": 3})
        r.zadd("{foo}b", {"a1": 1, "a2": 2})
        assert r.zdiffstore("{foo}out", ["{foo}a", "{foo}b"])
        assert r.zrange("{foo}out", 0, -1) == [b"a3"]
        response = r.zrange("{foo}out", 0, -1, withscores=True)
        assert_resp_response(r, response, [(b"a3", 3.0)], [[b"a3", 3.0]])

    @skip_if_server_version_lt("6.2.0")
    def test_cluster_zinter(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 2, "a3": 1})
        r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zinter(["{foo}a", "{foo}b", "{foo}c"]) == [b"a3", b"a1"]
        # invalid aggregation
        with pytest.raises(DataError):
            r.zinter(["{foo}a", "{foo}b", "{foo}c"], aggregate="foo", withscores=True)
        assert_resp_response(
            r,
            r.zinter(["{foo}a", "{foo}b", "{foo}c"], withscores=True),
            [(b"a3", 8), (b"a1", 9)],
            [[b"a3", 8], [b"a1", 9]],
        )
        assert_resp_response(
            r,
            r.zinter(["{foo}a", "{foo}b", "{foo}c"], withscores=True, aggregate="MAX"),
            [(b"a3", 5), (b"a1", 6)],
            [[b"a3", 5], [b"a1", 6]],
        )
        assert_resp_response(
            r,
            r.zinter(["{foo}a", "{foo}b", "{foo}c"], withscores=True, aggregate="MIN"),
            [(b"a1", 1), (b"a3", 1)],
            [[b"a1", 1], [b"a3", 1]],
        )
        assert_resp_response(
            r,
            r.zinter({"{foo}a": 1, "{foo}b": 2, "{foo}c": 3}, withscores=True),
            [(b"a3", 20.0), (b"a1", 23.0)],
            [[b"a3", 20.0], [b"a1", 23.0]],
        )

    def test_cluster_zinterstore_sum(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zinterstore("{foo}d", ["{foo}a", "{foo}b", "{foo}c"]) == 2
        assert_resp_response(
            r,
            r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a3", 8), (b"a1", 9)],
            [[b"a3", 8.0], [b"a1", 9.0]],
        )

    def test_cluster_zinterstore_max(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert (
            r.zinterstore("{foo}d", ["{foo}a", "{foo}b", "{foo}c"], aggregate="MAX")
            == 2
        )
        assert_resp_response(
            r,
            r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a3", 5), (b"a1", 6)],
            [[b"a3", 5.0], [b"a1", 6.0]],
        )

    def test_cluster_zinterstore_min(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 2, "a3": 3})
        r.zadd("{foo}b", {"a1": 2, "a2": 3, "a3": 5})
        r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert (
            r.zinterstore("{foo}d", ["{foo}a", "{foo}b", "{foo}c"], aggregate="MIN")
            == 2
        )
        assert_resp_response(
            r,
            r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a1", 1), (b"a3", 3)],
            [[b"a1", 1.0], [b"a3", 3.0]],
        )

    def test_cluster_zinterstore_with_weight(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zinterstore("{foo}d", {"{foo}a": 1, "{foo}b": 2, "{foo}c": 3}) == 2
        assert_resp_response(
            r,
            r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a3", 20), (b"a1", 23)],
            [[b"a3", 20.0], [b"a1", 23.0]],
        )

    @skip_if_server_version_lt("4.9.0")
    def test_cluster_bzpopmax(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 2})
        r.zadd("{foo}b", {"b1": 10, "b2": 20})
        assert_resp_response(
            r,
            r.bzpopmax(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}b", b"b2", 20),
            [b"{foo}b", b"b2", 20],
        )
        assert_resp_response(
            r,
            r.bzpopmax(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}b", b"b1", 10),
            [b"{foo}b", b"b1", 10],
        )
        assert_resp_response(
            r,
            r.bzpopmax(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}a", b"a2", 2),
            [b"{foo}a", b"a2", 2],
        )
        assert_resp_response(
            r,
            r.bzpopmax(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}a", b"a1", 1),
            [b"{foo}a", b"a1", 1],
        )
        assert r.bzpopmax(["{foo}b", "{foo}a"], timeout=1) is None
        r.zadd("{foo}c", {"c1": 100})
        assert_resp_response(
            r,
            r.bzpopmax("{foo}c", timeout=1),
            (b"{foo}c", b"c1", 100),
            [b"{foo}c", b"c1", 100],
        )

    @skip_if_server_version_lt("4.9.0")
    def test_cluster_bzpopmin(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 2})
        r.zadd("{foo}b", {"b1": 10, "b2": 20})
        assert_resp_response(
            r,
            r.bzpopmin(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}b", b"b1", 10),
            [b"{foo}b", b"b1", 10],
        )
        assert_resp_response(
            r,
            r.bzpopmin(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}b", b"b2", 20),
            [b"{foo}b", b"b2", 20],
        )
        assert_resp_response(
            r,
            r.bzpopmin(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}a", b"a1", 1),
            [b"{foo}a", b"a1", 1],
        )
        assert_resp_response(
            r,
            r.bzpopmin(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}a", b"a2", 2),
            [b"{foo}a", b"a2", 2],
        )
        assert r.bzpopmin(["{foo}b", "{foo}a"], timeout=1) is None
        r.zadd("{foo}c", {"c1": 100})
        assert_resp_response(
            r,
            r.bzpopmin("{foo}c", timeout=1),
            (b"{foo}c", b"c1", 100),
            [b"{foo}c", b"c1", 100],
        )

    @skip_if_server_version_lt("6.2.0")
    def test_cluster_zrangestore(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 2, "a3": 3})
        assert r.zrangestore("{foo}b", "{foo}a", 0, 1)
        assert r.zrange("{foo}b", 0, -1) == [b"a1", b"a2"]
        assert r.zrangestore("{foo}b", "{foo}a", 1, 2)
        assert r.zrange("{foo}b", 0, -1) == [b"a2", b"a3"]
        assert_resp_response(
            r,
            r.zrange("{foo}b", 0, 1, withscores=True),
            [(b"a2", 2), (b"a3", 3)],
            [[b"a2", 2.0], [b"a3", 3.0]],
        )
        # reversed order
        assert r.zrangestore("{foo}b", "{foo}a", 1, 2, desc=True)
        assert r.zrange("{foo}b", 0, -1) == [b"a1", b"a2"]
        # by score
        assert r.zrangestore(
            "{foo}b", "{foo}a", 2, 1, byscore=True, offset=0, num=1, desc=True
        )
        assert r.zrange("{foo}b", 0, -1) == [b"a2"]
        # by lex
        assert r.zrangestore(
            "{foo}b", "{foo}a", "[a2", "(a3", bylex=True, offset=0, num=1
        )
        assert r.zrange("{foo}b", 0, -1) == [b"a2"]

    @skip_if_server_version_lt("6.2.0")
    def test_cluster_zunion(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        # sum
        assert r.zunion(["{foo}a", "{foo}b", "{foo}c"]) == [b"a2", b"a4", b"a3", b"a1"]
        assert_resp_response(
            r,
            r.zunion(["{foo}a", "{foo}b", "{foo}c"], withscores=True),
            [(b"a2", 3), (b"a4", 4), (b"a3", 8), (b"a1", 9)],
            [[b"a2", 3.0], [b"a4", 4.0], [b"a3", 8.0], [b"a1", 9.0]],
        )
        # max
        assert_resp_response(
            r,
            r.zunion(["{foo}a", "{foo}b", "{foo}c"], aggregate="MAX", withscores=True),
            [(b"a2", 2), (b"a4", 4), (b"a3", 5), (b"a1", 6)],
            [[b"a2", 2.0], [b"a4", 4.0], [b"a3", 5.0], [b"a1", 6.0]],
        )
        # min
        assert_resp_response(
            r,
            r.zunion(["{foo}a", "{foo}b", "{foo}c"], aggregate="MIN", withscores=True),
            [(b"a1", 1), (b"a2", 1), (b"a3", 1), (b"a4", 4)],
            [[b"a1", 1.0], [b"a2", 1.0], [b"a3", 1.0], [b"a4", 4.0]],
        )
        # with weight
        assert_resp_response(
            r,
            r.zunion({"{foo}a": 1, "{foo}b": 2, "{foo}c": 3}, withscores=True),
            [(b"a2", 5), (b"a4", 12), (b"a3", 20), (b"a1", 23)],
            [[b"a2", 5.0], [b"a4", 12.0], [b"a3", 20.0], [b"a1", 23.0]],
        )

    def test_cluster_zunionstore_sum(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zunionstore("{foo}d", ["{foo}a", "{foo}b", "{foo}c"]) == 4
        assert_resp_response(
            r,
            r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a2", 3), (b"a4", 4), (b"a3", 8), (b"a1", 9)],
            [[b"a2", 3.0], [b"a4", 4.0], [b"a3", 8.0], [b"a1", 9.0]],
        )

    def test_cluster_zunionstore_max(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert (
            r.zunionstore("{foo}d", ["{foo}a", "{foo}b", "{foo}c"], aggregate="MAX")
            == 4
        )
        assert_resp_response(
            r,
            r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a2", 2), (b"a4", 4), (b"a3", 5), (b"a1", 6)],
            [[b"a2", 2.0], [b"a4", 4.0], [b"a3", 5.0], [b"a1", 6.0]],
        )

    def test_cluster_zunionstore_min(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 2, "a3": 3})
        r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 4})
        r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert (
            r.zunionstore("{foo}d", ["{foo}a", "{foo}b", "{foo}c"], aggregate="MIN")
            == 4
        )
        assert_resp_response(
            r,
            r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a1", 1), (b"a2", 2), (b"a3", 3), (b"a4", 4)],
            [[b"a1", 1.0], [b"a2", 2.0], [b"a3", 3.0], [b"a4", 4.0]],
        )

    def test_cluster_zunionstore_with_weight(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zunionstore("{foo}d", {"{foo}a": 1, "{foo}b": 2, "{foo}c": 3}) == 4
        assert_resp_response(
            r,
            r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a2", 5), (b"a4", 12), (b"a3", 20), (b"a1", 23)],
            [[b"a2", 5.0], [b"a4", 12.0], [b"a3", 20.0], [b"a1", 23.0]],
        )

    @skip_if_server_version_lt("2.8.9")
    def test_cluster_pfcount(self, r):
        members = {b"1", b"2", b"3"}
        r.pfadd("{foo}a", *members)
        assert r.pfcount("{foo}a") == len(members)
        members_b = {b"2", b"3", b"4"}
        r.pfadd("{foo}b", *members_b)
        assert r.pfcount("{foo}b") == len(members_b)
        assert r.pfcount("{foo}a", "{foo}b") == len(members_b.union(members))

    @skip_if_server_version_lt("2.8.9")
    def test_cluster_pfmerge(self, r):
        mema = {b"1", b"2", b"3"}
        memb = {b"2", b"3", b"4"}
        memc = {b"5", b"6", b"7"}
        r.pfadd("{foo}a", *mema)
        r.pfadd("{foo}b", *memb)
        r.pfadd("{foo}c", *memc)
        r.pfmerge("{foo}d", "{foo}c", "{foo}a")
        assert r.pfcount("{foo}d") == 6
        r.pfmerge("{foo}d", "{foo}b")
        assert r.pfcount("{foo}d") == 7

    def test_cluster_sort_store(self, r):
        r.rpush("{foo}a", "2", "3", "1")
        assert r.sort("{foo}a", store="{foo}sorted_values") == 3
        assert r.lrange("{foo}sorted_values", 0, -1) == [b"1", b"2", b"3"]

    # GEO COMMANDS
    @skip_if_server_version_lt("6.2.0")
    def test_cluster_geosearchstore(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        r.geoadd("{foo}barcelona", values)
        r.geosearchstore(
            "{foo}places_barcelona",
            "{foo}barcelona",
            longitude=2.191,
            latitude=41.433,
            radius=1000,
        )
        assert r.zrange("{foo}places_barcelona", 0, -1) == [b"place1"]

    @skip_unless_arch_bits(64)
    @skip_if_server_version_lt("6.2.0")
    def test_geosearchstore_dist(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        r.geoadd("{foo}barcelona", values)
        r.geosearchstore(
            "{foo}places_barcelona",
            "{foo}barcelona",
            longitude=2.191,
            latitude=41.433,
            radius=1000,
            storedist=True,
        )
        # instead of save the geo score, the distance is saved.
        assert r.zscore("{foo}places_barcelona", "place1") == 88.05060698409301

    @skip_if_server_version_lt("3.2.0")
    def test_cluster_georadius_store(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        r.geoadd("{foo}barcelona", values)
        r.georadius(
            "{foo}barcelona", 2.191, 41.433, 1000, store="{foo}places_barcelona"
        )
        assert r.zrange("{foo}places_barcelona", 0, -1) == [b"place1"]

    @skip_unless_arch_bits(64)
    @skip_if_server_version_lt("3.2.0")
    def test_cluster_georadius_store_dist(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        r.geoadd("{foo}barcelona", values)
        r.georadius(
            "{foo}barcelona", 2.191, 41.433, 1000, store_dist="{foo}places_barcelona"
        )
        # instead of save the geo score, the distance is saved.
        assert r.zscore("{foo}places_barcelona", "place1") == 88.05060698409301

    def test_cluster_dbsize(self, r):
        d = {"a": b"1", "b": b"2", "c": b"3", "d": b"4"}
        assert r.mset_nonatomic(d)
        assert r.dbsize(target_nodes="primaries") == len(d)

    def test_cluster_keys(self, r):
        assert r.keys() == []
        keys_with_underscores = {b"test_a", b"test_b"}
        keys = keys_with_underscores.union({b"testc"})
        for key in keys:
            r[key] = 1
        assert (
            set(r.keys(pattern="test_*", target_nodes="primaries"))
            == keys_with_underscores
        )
        assert set(r.keys(pattern="test*", target_nodes="primaries")) == keys

    # SCAN COMMANDS
    @skip_if_server_version_lt("2.8.0")
    def test_cluster_scan(self, r):
        r.set("a", 1)
        r.set("b", 2)
        r.set("c", 3)

        for target_nodes, nodes in zip(
            ["primaries", "replicas"], [r.get_primaries(), r.get_replicas()]
        ):
            cursors, keys = r.scan(target_nodes=target_nodes)
            assert sorted(keys) == [b"a", b"b", b"c"]
            assert sorted(cursors.keys()) == sorted(node.name for node in nodes)
            assert all(cursor == 0 for cursor in cursors.values())

            cursors, keys = r.scan(match="a*", target_nodes=target_nodes)
            assert sorted(keys) == [b"a"]
            assert sorted(cursors.keys()) == sorted(node.name for node in nodes)
            assert all(cursor == 0 for cursor in cursors.values())

    @skip_if_server_version_lt("6.0.0")
    def test_cluster_scan_type(self, r):
        r.sadd("a-set", 1)
        r.sadd("b-set", 1)
        r.sadd("c-set", 1)
        r.hset("a-hash", "foo", 2)
        r.lpush("a-list", "aux", 3)

        for target_nodes, nodes in zip(
            ["primaries", "replicas"], [r.get_primaries(), r.get_replicas()]
        ):
            cursors, keys = r.scan(_type="SET", target_nodes=target_nodes)
            assert sorted(keys) == [b"a-set", b"b-set", b"c-set"]
            assert sorted(cursors.keys()) == sorted(node.name for node in nodes)
            assert all(cursor == 0 for cursor in cursors.values())

            cursors, keys = r.scan(_type="SET", match="a*", target_nodes=target_nodes)
            assert sorted(keys) == [b"a-set"]
            assert sorted(cursors.keys()) == sorted(node.name for node in nodes)
            assert all(cursor == 0 for cursor in cursors.values())

    @skip_if_server_version_lt("2.8.0")
    def test_cluster_scan_iter(self, r):
        keys_all = []
        keys_1 = []
        for i in range(100):
            s = str(i)
            r.set(s, 1)
            keys_all.append(s.encode("utf-8"))
            if s.startswith("1"):
                keys_1.append(s.encode("utf-8"))
        keys_all.sort()
        keys_1.sort()

        for target_nodes in ["primaries", "replicas"]:
            keys = r.scan_iter(target_nodes=target_nodes)
            assert sorted(keys) == keys_all

            keys = r.scan_iter(match="1*", target_nodes=target_nodes)
            assert sorted(keys) == keys_1

    def test_cluster_randomkey(self, r):
        node = r.get_node_from_key("{foo}")
        assert r.randomkey(target_nodes=node) is None
        for key in ("{foo}a", "{foo}b", "{foo}c"):
            r[key] = 1
        assert r.randomkey(target_nodes=node) in (b"{foo}a", b"{foo}b", b"{foo}c")

    @skip_if_server_version_lt("6.0.0")
    @skip_if_redis_enterprise()
    def test_acl_log(self, r, request):
        key = "{cache}:"
        node = r.get_node_from_key(key)
        username = "redis-py-user"

        def teardown():
            r.acl_deluser(username, target_nodes="primaries")

        request.addfinalizer(teardown)
        r.acl_setuser(
            username,
            enabled=True,
            reset=True,
            commands=["+get", "+set", "+select", "+cluster", "+command", "+info"],
            keys=["{cache}:*"],
            nopass=True,
            target_nodes="primaries",
        )
        r.acl_log_reset(target_nodes=node)

        user_client = _get_client(
            RedisCluster, request, flushdb=False, username=username
        )

        # Valid operation and key
        assert user_client.set("{cache}:0", 1)
        assert user_client.get("{cache}:0") == b"1"

        # Invalid key
        with pytest.raises(NoPermissionError):
            user_client.get("{cache}violated_cache:0")

        # Invalid operation
        with pytest.raises(NoPermissionError):
            user_client.hset("{cache}:0", "hkey", "hval")

        assert isinstance(r.acl_log(target_nodes=node), list)
        assert len(r.acl_log(target_nodes=node)) == 3
        assert len(r.acl_log(count=1, target_nodes=node)) == 1
        assert isinstance(r.acl_log(target_nodes=node)[0], dict)
        assert "client-info" in r.acl_log(count=1, target_nodes=node)[0]
        assert r.acl_log_reset(target_nodes=node)

    def generate_lib_code(self, lib_name):
        return f"""#!js api_version=1.0 name={lib_name}\n redis.registerFunction('foo', ()=>{{return 'bar'}})"""  # noqa

    def try_delete_libs(self, r, *lib_names):
        for lib_name in lib_names:
            try:
                r.tfunction_delete(lib_name)
            except Exception:
                pass

    @pytest.mark.redismod
    @skip_if_server_version_lt("7.1.140")
    def test_tfunction_load_delete(self, r):
        r.gears_refresh_cluster()
        self.try_delete_libs(r, "lib1")
        lib_code = self.generate_lib_code("lib1")
        assert r.tfunction_load(lib_code)
        assert r.tfunction_delete("lib1")

    @pytest.mark.redismod
    @skip_if_server_version_lt("7.1.140")
    def test_tfunction_list(self, r):
        r.gears_refresh_cluster()
        self.try_delete_libs(r, "lib1", "lib2", "lib3")
        assert r.tfunction_load(self.generate_lib_code("lib1"))
        assert r.tfunction_load(self.generate_lib_code("lib2"))
        assert r.tfunction_load(self.generate_lib_code("lib3"))

        # test error thrown when verbose > 4
        with pytest.raises(DataError):
            assert r.tfunction_list(verbose=8)

        functions = r.tfunction_list(verbose=1)
        assert len(functions) == 3

        expected_names = [b"lib1", b"lib2", b"lib3"]
        actual_names = [functions[0][13], functions[1][13], functions[2][13]]

        assert sorted(expected_names) == sorted(actual_names)
        assert r.tfunction_delete("lib1")
        assert r.tfunction_delete("lib2")
        assert r.tfunction_delete("lib3")

    @pytest.mark.redismod
    @skip_if_server_version_lt("7.1.140")
    def test_tfcall(self, r):
        r.gears_refresh_cluster()
        self.try_delete_libs(r, "lib1")
        assert r.tfunction_load(self.generate_lib_code("lib1"))
        assert r.tfcall("lib1", "foo") == b"bar"
        assert r.tfcall_async("lib1", "foo") == b"bar"

        assert r.tfunction_delete("lib1")


@pytest.mark.onlycluster
class TestNodesManager:
    """
    Tests for the NodesManager class
    """

    def test_load_balancer(self, r):
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

    def test_init_slots_cache_not_all_slots_covered(self):
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
            get_mocked_redis_client(
                host=default_host,
                port=default_port,
                cluster_slots=cluster_slots,
                require_full_coverage=True,
            )
        assert str(ex.value).startswith(
            "All slots are not covered after query all startup_nodes."
        )

    def test_init_slots_cache_not_require_full_coverage_success(self):
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

        rc = get_mocked_redis_client(
            host=default_host,
            port=default_port,
            cluster_slots=cluster_slots,
            require_full_coverage=False,
        )

        assert 5460 not in rc.nodes_manager.slots_cache

    def test_init_slots_cache(self):
        """
        Test that slots cache can in initialized and all slots are covered
        """
        good_slots_resp = [
            [0, 5460, ["127.0.0.1", 7000], ["127.0.0.2", 7003]],
            [5461, 10922, ["127.0.0.1", 7001], ["127.0.0.2", 7004]],
            [10923, 16383, ["127.0.0.1", 7002], ["127.0.0.2", 7005]],
        ]

        rc = get_mocked_redis_client(
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

    def test_init_promote_server_type_for_node_in_cache(self):
        """
        When replica is promoted to master, nodes_cache must change the server type
        accordingly
        """
        cluster_slots_before_promotion = [
            [0, 16383, ["127.0.0.1", 7000], ["127.0.0.1", 7003]]
        ]
        cluster_slots_after_promotion = [
            [0, 16383, ["127.0.0.1", 7003], ["127.0.0.1", 7004]]
        ]

        cluster_slots_results = [
            cluster_slots_before_promotion,
            cluster_slots_after_promotion,
        ]

        with patch.object(Redis, "execute_command") as execute_command_mock:

            def execute_command(*_args, **_kwargs):
                if _args[0] == "CLUSTER SLOTS":
                    mock_cluster_slots = cluster_slots_results.pop(0)
                    return mock_cluster_slots
                elif _args[0] == "COMMAND":
                    return {"get": [], "set": []}
                elif _args[0] == "INFO":
                    return {"cluster_enabled": True}
                elif len(_args) > 1 and _args[1] == "cluster-require-full-coverage":
                    return {"cluster-require-full-coverage": False}
                else:
                    return execute_command_mock(*_args, **_kwargs)

            execute_command_mock.side_effect = execute_command

            nm = NodesManager(
                startup_nodes=[ClusterNode(host=default_host, port=default_port)],
                from_url=False,
                require_full_coverage=False,
                dynamic_startup_nodes=True,
            )

            assert nm.default_node.host == "127.0.0.1"
            assert nm.default_node.port == 7000
            assert nm.default_node.server_type == PRIMARY

            nm.initialize()

            assert nm.default_node.host == "127.0.0.1"
            assert nm.default_node.port == 7003
            assert nm.default_node.server_type == PRIMARY

    def test_init_slots_cache_cluster_mode_disabled(self):
        """
        Test that creating a RedisCluster failes if one of the startup nodes
        has cluster mode disabled
        """
        with pytest.raises(RedisClusterException) as e:
            get_mocked_redis_client(
                cluster_slots_raise_error=True,
                host=default_host,
                port=default_port,
                cluster_enabled=False,
            )
            assert "Cluster mode is not enabled on this node" in str(e.value)

    def test_empty_startup_nodes(self):
        """
        It should not be possible to create a node manager with no nodes
        specified
        """
        with pytest.raises(RedisClusterException):
            NodesManager([])

    def test_wrong_startup_nodes_type(self):
        """
        If something other then a list type itteratable is provided it should
        fail
        """
        with pytest.raises(RedisClusterException):
            NodesManager({})

    def test_init_slots_cache_slots_collision(self, request):
        """
        Test that if 2 nodes do not agree on the same slots setup it should
        raise an error. In this test both nodes will say that the first
        slots block should be bound to different servers.
        """
        with patch.object(NodesManager, "create_redis_node") as create_redis_node:

            def create_mocked_redis_node(host, port, **kwargs):
                """
                Helper function to return custom slots cache_data data from
                different redis nodes
                """
                if port == 7000:
                    result = [
                        [0, 5460, ["127.0.0.1", 7000], ["127.0.0.1", 7003]],
                        [5461, 10922, ["127.0.0.1", 7001], ["127.0.0.1", 7004]],
                    ]

                elif port == 7001:
                    result = [
                        [0, 5460, ["127.0.0.1", 7001], ["127.0.0.1", 7003]],
                        [5461, 10922, ["127.0.0.1", 7000], ["127.0.0.1", 7004]],
                    ]
                else:
                    result = []

                r_node = Redis(host=host, port=port)

                orig_execute_command = r_node.execute_command

                def execute_command(*args, **kwargs):
                    if args[0] == "CLUSTER SLOTS":
                        return result
                    elif args[0] == "INFO":
                        return {"cluster_enabled": True}
                    elif args[1] == "cluster-require-full-coverage":
                        return {"cluster-require-full-coverage": "yes"}
                    else:
                        return orig_execute_command(*args, **kwargs)

                r_node.execute_command = execute_command
                return r_node

            create_redis_node.side_effect = create_mocked_redis_node

            with pytest.raises(RedisClusterException) as ex:
                node_1 = ClusterNode("127.0.0.1", 7000)
                node_2 = ClusterNode("127.0.0.1", 7001)
                RedisCluster(startup_nodes=[node_1, node_2])
            assert str(ex.value).startswith(
                "startup_nodes could not agree on a valid slots cache"
            ), str(ex.value)

    def test_cluster_one_instance(self):
        """
        If the cluster exists of only 1 node then there is some hacks that must
        be validated they work.
        """
        node = ClusterNode(default_host, default_port)
        cluster_slots = [[0, 16383, ["", default_port]]]
        rc = get_mocked_redis_client(startup_nodes=[node], cluster_slots=cluster_slots)

        n = rc.nodes_manager
        assert len(n.nodes_cache) == 1
        n_node = rc.get_node(node_name=node.name)
        assert n_node is not None
        assert n_node == node
        assert n_node.server_type == PRIMARY
        assert len(n.slots_cache) == REDIS_CLUSTER_HASH_SLOTS
        for i in range(0, REDIS_CLUSTER_HASH_SLOTS):
            assert n.slots_cache[i] == [n_node]

    def test_init_with_down_node(self):
        """
        If I can't connect to one of the nodes, everything should still work.
        But if I can't connect to any of the nodes, exception should be thrown.
        """
        with patch.object(NodesManager, "create_redis_node") as create_redis_node:

            def create_mocked_redis_node(host, port, **kwargs):
                if port == 7000:
                    raise ConnectionError("mock connection error for 7000")

                r_node = Redis(host=host, port=port, decode_responses=True)

                def execute_command(*args, **kwargs):
                    if args[0] == "CLUSTER SLOTS":
                        return [
                            [0, 8191, ["127.0.0.1", 7001, "node_1"]],
                            [8192, 16383, ["127.0.0.1", 7002, "node_2"]],
                        ]
                    elif args[0] == "INFO":
                        return {"cluster_enabled": True}
                    elif args[1] == "cluster-require-full-coverage":
                        return {"cluster-require-full-coverage": "yes"}

                r_node.execute_command = execute_command

                return r_node

            create_redis_node.side_effect = create_mocked_redis_node

            node_1 = ClusterNode("127.0.0.1", 7000)
            node_2 = ClusterNode("127.0.0.1", 7001)

            # If all startup nodes fail to connect, connection error should be
            # thrown
            with pytest.raises(RedisClusterException) as e:
                RedisCluster(startup_nodes=[node_1])
            assert "Redis Cluster cannot be connected" in str(e.value)

            with patch.object(
                CommandsParser, "initialize", autospec=True
            ) as cmd_parser_initialize:

                def cmd_init_mock(self, r):
                    self.commands = {
                        "get": {
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
                rc = RedisCluster(startup_nodes=[node_1, node_2])
                assert rc.get_node(host=default_host, port=7001) is not None
                assert rc.get_node(host=default_host, port=7002) is not None

    @pytest.mark.parametrize("dynamic_startup_nodes", [True, False])
    def test_init_slots_dynamic_startup_nodes(self, dynamic_startup_nodes):
        rc = get_mocked_redis_client(
            host="my@DNS.com",
            port=7000,
            cluster_slots=default_cluster_slots,
            dynamic_startup_nodes=dynamic_startup_nodes,
        )
        # Nodes are taken from default_cluster_slots
        discovered_nodes = [
            "127.0.0.1:7000",
            "127.0.0.1:7001",
            "127.0.0.1:7002",
            "127.0.0.1:7003",
        ]
        startup_nodes = list(rc.nodes_manager.startup_nodes.keys())
        if dynamic_startup_nodes is True:
            assert startup_nodes.sort() == discovered_nodes.sort()
        else:
            assert startup_nodes == ["my@DNS.com:7000"]

    @pytest.mark.parametrize(
        "connection_pool_class", [ConnectionPool, BlockingConnectionPool]
    )
    def test_connection_pool_class(self, connection_pool_class):
        rc = get_mocked_redis_client(
            url="redis://my@DNS.com:7000",
            cluster_slots=default_cluster_slots,
            connection_pool_class=connection_pool_class,
        )

        for node in rc.nodes_manager.nodes_cache.values():
            assert isinstance(
                node.redis_connection.connection_pool, connection_pool_class
            )

    @pytest.mark.parametrize("queue_class", [Queue, LifoQueue])
    def test_allow_custom_queue_class(self, queue_class):
        rc = get_mocked_redis_client(
            url="redis://my@DNS.com:7000",
            cluster_slots=default_cluster_slots,
            connection_pool_class=BlockingConnectionPool,
            queue_class=queue_class,
        )

        for node in rc.nodes_manager.nodes_cache.values():
            assert node.redis_connection.connection_pool.queue_class == queue_class


@pytest.mark.onlycluster
class TestClusterPubSubObject:
    """
    Tests for the ClusterPubSub class
    """

    def test_init_pubsub_with_host_and_port(self, r):
        """
        Test creation of pubsub instance with passed host and port
        """
        node = r.get_default_node()
        p = r.pubsub(host=node.host, port=node.port)
        assert p.get_pubsub_node() == node

    def test_init_pubsub_with_node(self, r):
        """
        Test creation of pubsub instance with passed node
        """
        node = r.get_default_node()
        p = r.pubsub(node=node)
        assert p.get_pubsub_node() == node

    def test_init_pubusub_without_specifying_node(self, r):
        """
        Test creation of pubsub instance without specifying a node. The node
        should be determined based on the keyslot of the first command
        execution.
        """
        channel_name = "foo"
        node = r.get_node_from_key(channel_name)
        p = r.pubsub()
        assert p.get_pubsub_node() is None
        p.subscribe(channel_name)
        assert p.get_pubsub_node() == node

    def test_init_pubsub_with_a_non_existent_node(self, r):
        """
        Test creation of pubsub instance with node that doesn't exists in the
        cluster. RedisClusterException should be raised.
        """
        node = ClusterNode("1.1.1.1", 1111)
        with pytest.raises(RedisClusterException):
            r.pubsub(node)

    def test_init_pubsub_with_a_non_existent_host_port(self, r):
        """
        Test creation of pubsub instance with host and port that don't belong
        to a node in the cluster.
        RedisClusterException should be raised.
        """
        with pytest.raises(RedisClusterException):
            r.pubsub(host="1.1.1.1", port=1111)

    def test_init_pubsub_host_or_port(self, r):
        """
        Test creation of pubsub instance with host but without port, and vice
        versa. DataError should be raised.
        """
        with pytest.raises(DataError):
            r.pubsub(host="localhost")

        with pytest.raises(DataError):
            r.pubsub(port=16379)

    def test_get_redis_connection(self, r):
        """
        Test that get_redis_connection() returns the redis connection of the
        set pubsub node
        """
        node = r.get_default_node()
        p = r.pubsub(node=node)
        assert p.get_redis_connection() == node.redis_connection


@pytest.mark.onlycluster
class TestClusterPipeline:
    """
    Tests for the ClusterPipeline class
    """

    def test_blocked_methods(self, r):
        """
        Currently some method calls on a Cluster pipeline
        is blocked when using in cluster mode.
        They maybe implemented in the future.
        """
        pipe = r.pipeline()
        with pytest.raises(RedisClusterException):
            pipe.multi()

        with pytest.raises(RedisClusterException):
            pipe.immediate_execute_command()

        with pytest.raises(RedisClusterException):
            pipe._execute_transaction(None, None, None)

        with pytest.raises(RedisClusterException):
            pipe.load_scripts()

        with pytest.raises(RedisClusterException):
            pipe.watch()

        with pytest.raises(RedisClusterException):
            pipe.unwatch()

        with pytest.raises(RedisClusterException):
            pipe.script_load_for_pipeline(None)

        with pytest.raises(RedisClusterException):
            pipe.eval()

    def test_blocked_arguments(self, r):
        """
        Currently some arguments is blocked when using in cluster mode.
        They maybe implemented in the future.
        """
        with pytest.raises(RedisClusterException) as ex:
            r.pipeline(transaction=True)

        assert (
            str(ex.value).startswith("transaction is deprecated in cluster mode")
            is True
        )

        with pytest.raises(RedisClusterException) as ex:
            r.pipeline(shard_hint=True)

        assert (
            str(ex.value).startswith("shard_hint is deprecated in cluster mode") is True
        )

    def test_redis_cluster_pipeline(self, r):
        """
        Test that we can use a pipeline with the RedisCluster class
        """
        with r.pipeline() as pipe:
            pipe.set("foo", "bar")
            pipe.get("foo")
            assert pipe.execute() == [True, b"bar"]

    def test_mget_disabled(self, r):
        """
        Test that mget is disabled for ClusterPipeline
        """
        with r.pipeline() as pipe:
            with pytest.raises(RedisClusterException):
                pipe.mget(["a"])

    def test_mset_disabled(self, r):
        """
        Test that mset is disabled for ClusterPipeline
        """
        with r.pipeline() as pipe:
            with pytest.raises(RedisClusterException):
                pipe.mset({"a": 1, "b": 2})

    def test_rename_disabled(self, r):
        """
        Test that rename is disabled for ClusterPipeline
        """
        with r.pipeline(transaction=False) as pipe:
            with pytest.raises(RedisClusterException):
                pipe.rename("a", "b")

    def test_renamenx_disabled(self, r):
        """
        Test that renamenx is disabled for ClusterPipeline
        """
        with r.pipeline(transaction=False) as pipe:
            with pytest.raises(RedisClusterException):
                pipe.renamenx("a", "b")

    def test_delete_single(self, r):
        """
        Test a single delete operation
        """
        r["a"] = 1
        with r.pipeline(transaction=False) as pipe:
            pipe.delete("a")
            assert pipe.execute() == [1]

    def test_multi_delete_unsupported(self, r):
        """
        Test that multi delete operation is unsupported
        """
        with r.pipeline(transaction=False) as pipe:
            r["a"] = 1
            r["b"] = 2
            with pytest.raises(RedisClusterException):
                pipe.delete("a", "b")

    def test_unlink_single(self, r):
        """
        Test a single unlink operation
        """
        r["a"] = 1
        with r.pipeline(transaction=False) as pipe:
            pipe.unlink("a")
            assert pipe.execute() == [1]

    def test_multi_unlink_unsupported(self, r):
        """
        Test that multi unlink operation is unsupported
        """
        with r.pipeline(transaction=False) as pipe:
            r["a"] = 1
            r["b"] = 2
            with pytest.raises(RedisClusterException):
                pipe.unlink("a", "b")

    def test_brpoplpush_disabled(self, r):
        """
        Test that brpoplpush is disabled for ClusterPipeline
        """
        with r.pipeline(transaction=False) as pipe:
            with pytest.raises(RedisClusterException):
                pipe.brpoplpush()

    def test_rpoplpush_disabled(self, r):
        """
        Test that rpoplpush is disabled for ClusterPipeline
        """
        with r.pipeline(transaction=False) as pipe:
            with pytest.raises(RedisClusterException):
                pipe.rpoplpush()

    def test_sort_disabled(self, r):
        """
        Test that sort is disabled for ClusterPipeline
        """
        with r.pipeline(transaction=False) as pipe:
            with pytest.raises(RedisClusterException):
                pipe.sort()

    def test_sdiff_disabled(self, r):
        """
        Test that sdiff is disabled for ClusterPipeline
        """
        with r.pipeline(transaction=False) as pipe:
            with pytest.raises(RedisClusterException):
                pipe.sdiff()

    def test_sdiffstore_disabled(self, r):
        """
        Test that sdiffstore is disabled for ClusterPipeline
        """
        with r.pipeline(transaction=False) as pipe:
            with pytest.raises(RedisClusterException):
                pipe.sdiffstore()

    def test_sinter_disabled(self, r):
        """
        Test that sinter is disabled for ClusterPipeline
        """
        with r.pipeline(transaction=False) as pipe:
            with pytest.raises(RedisClusterException):
                pipe.sinter()

    def test_sinterstore_disabled(self, r):
        """
        Test that sinterstore is disabled for ClusterPipeline
        """
        with r.pipeline(transaction=False) as pipe:
            with pytest.raises(RedisClusterException):
                pipe.sinterstore()

    def test_smove_disabled(self, r):
        """
        Test that move is disabled for ClusterPipeline
        """
        with r.pipeline(transaction=False) as pipe:
            with pytest.raises(RedisClusterException):
                pipe.smove()

    def test_sunion_disabled(self, r):
        """
        Test that sunion is disabled for ClusterPipeline
        """
        with r.pipeline(transaction=False) as pipe:
            with pytest.raises(RedisClusterException):
                pipe.sunion()

    def test_sunionstore_disabled(self, r):
        """
        Test that sunionstore is disabled for ClusterPipeline
        """
        with r.pipeline(transaction=False) as pipe:
            with pytest.raises(RedisClusterException):
                pipe.sunionstore()

    def test_spfmerge_disabled(self, r):
        """
        Test that spfmerge is disabled for ClusterPipeline
        """
        with r.pipeline(transaction=False) as pipe:
            with pytest.raises(RedisClusterException):
                pipe.pfmerge()

    def test_multi_key_operation_with_a_single_slot(self, r):
        """
        Test multi key operation with a single slot
        """
        pipe = r.pipeline(transaction=False)
        pipe.set("a{foo}", 1)
        pipe.set("b{foo}", 2)
        pipe.set("c{foo}", 3)
        pipe.get("a{foo}")
        pipe.get("b{foo}")
        pipe.get("c{foo}")

        res = pipe.execute()
        assert res == [True, True, True, b"1", b"2", b"3"]

    def test_multi_key_operation_with_multi_slots(self, r):
        """
        Test multi key operation with more than one slot
        """
        pipe = r.pipeline(transaction=False)
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
        res = pipe.execute()
        assert res == [True, True, True, True, True, b"1", b"2", b"3", b"4", b"5"]

    def test_connection_error_not_raised(self, r):
        """
        Test that the pipeline doesn't raise an error on connection error when
        raise_on_error=False
        """
        key = "foo"
        node = r.get_node_from_key(key, False)

        def raise_connection_error():
            e = ConnectionError("error")
            return e

        with r.pipeline() as pipe:
            mock_node_resp_func(node, raise_connection_error)
            res = pipe.get(key).get(key).execute(raise_on_error=False)
            assert node.redis_connection.connection.read_response.called
            assert isinstance(res[0], ConnectionError)

    def test_connection_error_raised(self, r):
        """
        Test that the pipeline raises an error on connection error when
        raise_on_error=True
        """
        key = "foo"
        node = r.get_node_from_key(key, False)

        def raise_connection_error():
            e = ConnectionError("error")
            return e

        with r.pipeline() as pipe:
            mock_node_resp_func(node, raise_connection_error)
            with pytest.raises(ConnectionError):
                pipe.get(key).get(key).execute(raise_on_error=True)

    def test_asking_error(self, r):
        """
        Test redirection on ASK error
        """
        key = "foo"
        first_node = r.get_node_from_key(key, False)
        ask_node = None
        for node in r.get_nodes():
            if node != first_node:
                ask_node = node
                break
        if ask_node is None:
            warnings.warn("skipping this test since the cluster has only one node")
            return
        ask_msg = f"{r.keyslot(key)} {ask_node.host}:{ask_node.port}"

        def raise_ask_error():
            raise AskError(ask_msg)

        with r.pipeline() as pipe:
            mock_node_resp_func(first_node, raise_ask_error)
            mock_node_resp(ask_node, "MOCK_OK")
            res = pipe.get(key).execute()
            assert first_node.redis_connection.connection.read_response.called
            assert ask_node.redis_connection.connection.read_response.called
            assert res == ["MOCK_OK"]

    def test_return_previously_acquired_connections(self, r):
        # in order to ensure that a pipeline will make use of connections
        #   from different nodes
        assert r.keyslot("a") != r.keyslot("b")

        orig_func = redis.cluster.get_connection
        with patch("redis.cluster.get_connection") as get_connection:

            def raise_error(target_node, *args, **kwargs):
                if get_connection.call_count == 2:
                    raise ConnectionError("mocked error")
                else:
                    return orig_func(target_node, *args, **kwargs)

            get_connection.side_effect = raise_error

            r.pipeline().get("a").get("b").execute()

        # 4 = 2 get_connections per execution * 2 executions
        assert get_connection.call_count == 4
        for cluster_node in r.nodes_manager.nodes_cache.values():
            connection_pool = cluster_node.redis_connection.connection_pool
            num_of_conns = len(connection_pool._available_connections)
            assert num_of_conns == connection_pool._created_connections

    def test_empty_stack(self, r):
        """
        If pipeline is executed with no commands it should
        return a empty list.
        """
        p = r.pipeline()
        result = p.execute()
        assert result == []


@pytest.mark.onlycluster
class TestReadOnlyPipeline:
    """
    Tests for ClusterPipeline class in readonly mode
    """

    def test_pipeline_readonly(self, r):
        """
        On readonly mode, we supports get related stuff only.
        """
        r.readonly(target_nodes="all")
        r.set("foo71", "a1")  # we assume this key is set on 127.0.0.1:7001
        r.zadd("foo88", {"z1": 1})  # we assume this key is set on 127.0.0.1:7002
        r.zadd("foo88", {"z2": 4})

        with r.pipeline() as readonly_pipe:
            readonly_pipe.get("foo71").zrange("foo88", 0, 5, withscores=True)
            assert_resp_response(
                r,
                readonly_pipe.execute(),
                [b"a1", [(b"z1", 1.0), (b"z2", 4)]],
                [b"a1", [[b"z1", 1.0], [b"z2", 4.0]]],
            )

    def test_moved_redirection_on_slave_with_default(self, r):
        """
        On Pipeline, we redirected once and finally get from master with
        readonly client when data is completely moved.
        """
        key = "bar"
        r.set(key, "foo")
        # set read_from_replicas to True
        r.read_from_replicas = True
        primary = r.get_node_from_key(key, False)
        replica = r.get_node_from_key(key, True)
        with r.pipeline() as readwrite_pipe:
            mock_node_resp(primary, "MOCK_FOO")
            if replica is not None:
                moved_error = f"{r.keyslot(key)} {primary.host}:{primary.port}"

                def raise_moved_error():
                    raise MovedError(moved_error)

                mock_node_resp_func(replica, raise_moved_error)
            assert readwrite_pipe.reinitialize_counter == 0
            readwrite_pipe.get(key).get(key)
            assert readwrite_pipe.execute() == ["MOCK_FOO", "MOCK_FOO"]
            if replica is not None:
                # the slot has a replica as well, so MovedError should have
                # occurred. If MovedError occurs, we should see the
                # reinitialize_counter increase.
                assert readwrite_pipe.reinitialize_counter == 1
                conn = replica.redis_connection.connection
                assert conn.read_response.called is True

    def test_readonly_pipeline_from_readonly_client(self, request):
        """
        Test that the pipeline is initialized with readonly mode if the client
        has it enabled
        """
        # Create a cluster with reading from replications
        ro = _get_client(RedisCluster, request, read_from_replicas=True)
        key = "bar"
        ro.set(key, "foo")
        import time

        time.sleep(0.2)
        with ro.pipeline() as readonly_pipe:
            mock_all_nodes_resp(ro, "MOCK_OK")
            assert readonly_pipe.read_from_replicas is True
            assert readonly_pipe.get(key).get(key).execute() == ["MOCK_OK", "MOCK_OK"]
            slot_nodes = ro.nodes_manager.slots_cache[ro.keyslot(key)]
            if len(slot_nodes) > 1:
                executed_on_replica = False
                for node in slot_nodes:
                    if node.server_type == REPLICA:
                        conn = node.redis_connection.connection
                        executed_on_replica = conn.read_response.called
                        if executed_on_replica:
                            break
                assert executed_on_replica is True


@pytest.mark.onlycluster
class TestClusterMonitor:
    def test_wait_command_not_found(self, r):
        "Make sure the wait_for_command func works when command is not found"
        key = "foo"
        node = r.get_node_from_key(key)
        with r.monitor(target_node=node) as m:
            response = wait_for_command(r, m, "nothing", key=key)
            assert response is None

    def test_response_values(self, r):
        db = 0
        key = "foo"
        node = r.get_node_from_key(key)
        with r.monitor(target_node=node) as m:
            r.ping(target_nodes=node)
            response = wait_for_command(r, m, "PING", key=key)
            assert isinstance(response["time"], float)
            assert response["db"] == db
            assert response["client_type"] in ("tcp", "unix")
            assert isinstance(response["client_address"], str)
            assert isinstance(response["client_port"], str)
            assert response["command"] == "PING"

    def test_command_with_quoted_key(self, r):
        key = "{foo}1"
        node = r.get_node_from_key(key)
        with r.monitor(node) as m:
            r.get('{foo}"bar')
            response = wait_for_command(r, m, 'GET {foo}"bar', key=key)
            assert response["command"] == 'GET {foo}"bar'

    def test_command_with_binary_data(self, r):
        key = "{foo}1"
        node = r.get_node_from_key(key)
        with r.monitor(target_node=node) as m:
            byte_string = b"{foo}bar\x92"
            r.get(byte_string)
            response = wait_for_command(r, m, "GET {foo}bar\\x92", key=key)
            assert response["command"] == "GET {foo}bar\\x92"

    def test_command_with_escaped_data(self, r):
        key = "{foo}1"
        node = r.get_node_from_key(key)
        with r.monitor(target_node=node) as m:
            byte_string = b"{foo}bar\\x92"
            r.get(byte_string)
            response = wait_for_command(r, m, "GET {foo}bar\\\\x92", key=key)
            assert response["command"] == "GET {foo}bar\\\\x92"

    def test_flush(self, r):
        r.set("x", "1")
        r.set("z", "1")
        r.flushall()
        assert r.get("x") is None
        assert r.get("y") is None
