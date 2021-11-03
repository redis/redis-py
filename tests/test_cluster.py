import pytest
import datetime
import warnings

from time import sleep
from unittest.mock import call, patch, DEFAULT, Mock
from redis import Redis
from redis.cluster import get_node_name, ClusterNode, \
    RedisCluster, NodesManager, PRIMARY, REDIS_CLUSTER_HASH_SLOTS, REPLICA
from redis.commands import CommandsParser
from redis.connection import Connection
from redis.utils import str_if_bytes
from redis.exceptions import (
    AskError,
    ClusterDownError,
    MovedError,
    RedisClusterException,
    RedisError
)

from redis.crc import key_slot
from .conftest import (
    skip_if_not_cluster_mode,
    _get_client,
    skip_if_server_version_lt
)

default_host = "127.0.0.1"
default_port = 7000
default_cluster_slots = [
    [
        0, 8191,
        ['127.0.0.1', 7000, 'node_0'],
        ['127.0.0.1', 7003, 'node_3'],
    ],
    [
        8192, 16383,
        ['127.0.0.1', 7001, 'node_1'],
        ['127.0.0.1', 7002, 'node_2']
    ]
]


@pytest.fixture()
def slowlog(request, r):
    """
    Set the slowlog threshold to 0, and the
    max length to 128. This will force every
    command into the slowlog and allow us
    to test it
    """
    # Save old values
    current_config = r.config_get(
        target_nodes=r.get_primaries()[0])
    old_slower_than_value = current_config['slowlog-log-slower-than']
    old_max_legnth_value = current_config['slowlog-max-len']

    # Function to restore the old values
    def cleanup():
        r.config_set('slowlog-log-slower-than', old_slower_than_value)
        r.config_set('slowlog-max-len', old_max_legnth_value)
    request.addfinalizer(cleanup)

    # Set the new values
    r.config_set('slowlog-log-slower-than', 0)
    r.config_set('slowlog-max-len', 128)


def get_mocked_redis_client(func=None, *args, **kwargs):
    """
    Return a stable RedisCluster object that have deterministic
    nodes and slots setup to remove the problem of different IP addresses
    on different installations and machines.
    """
    cluster_slots = kwargs.pop('cluster_slots', default_cluster_slots)
    coverage_res = kwargs.pop('coverage_result', 'yes')
    with patch.object(Redis, 'execute_command') as execute_command_mock:
        def execute_command(*_args, **_kwargs):
            if _args[0] == 'CLUSTER SLOTS':
                mock_cluster_slots = cluster_slots
                return mock_cluster_slots
            elif _args[1] == 'cluster-require-full-coverage':
                return {'cluster-require-full-coverage': coverage_res}
            elif func is not None:
                return func(*args, **kwargs)
            else:
                return execute_command_mock(*_args, **_kwargs)

        execute_command_mock.side_effect = execute_command

        with patch.object(CommandsParser, 'initialize',
                          autospec=True) as cmd_parser_initialize:

            def cmd_init_mock(self, r):
                self.commands = {'get': {'name': 'get', 'arity': 2,
                                         'flags': ['readonly',
                                                   'fast'],
                                         'first_key_pos': 1,
                                         'last_key_pos': 1,
                                         'step_count': 1}}

            cmd_parser_initialize.side_effect = cmd_init_mock

            return RedisCluster(*args, **kwargs)


def mock_node_resp(node, response):
    connection = Mock()
    connection.read_response.return_value = response
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
            warnings.warn("Skipping this test since it requires to have a "
                          "replica")
            return
        redirect_node = rc.nodes_manager.slots_cache[slot][1]
    else:
        # Use one of the primaries to be the redirected node
        redirect_node = rc.get_primaries()[0]
    r_host = redirect_node.host
    r_port = redirect_node.port
    with patch.object(Redis, 'parse_response') as parse_response:
        def moved_redirect_effect(connection, *args, **options):
            def ok_response(connection, *args, **options):
                assert connection.host == r_host
                assert connection.port == r_port

                return "MOCK_OK"

            parse_response.side_effect = ok_response
            raise MovedError("{0} {1}:{2}".format(slot, r_host, r_port))

        parse_response.side_effect = moved_redirect_effect
        assert rc.execute_command("SET", "foo", "bar") == "MOCK_OK"
        slot_primary = rc.nodes_manager.slots_cache[slot][0]
        assert slot_primary == redirect_node
        if failover:
            assert rc.get_node(host=r_host, port=r_port).server_type == PRIMARY
            assert prev_primary.server_type == REPLICA


@skip_if_not_cluster_mode()
class TestRedisClusterObj:
    def test_host_port_startup_node(self):
        """
        Test that it is possible to use host & port arguments as startup node
        args
        """
        cluster = get_mocked_redis_client(host=default_host, port=default_port)
        assert cluster.get_node(host=default_host,
                                port=default_port) is not None

    def test_startup_nodes(self):
        """
        Test that it is possible to use startup_nodes
        argument to init the cluster
        """
        port_1 = 7000
        port_2 = 7001
        startup_nodes = [ClusterNode(default_host, port_1),
                         ClusterNode(default_host, port_2)]
        cluster = get_mocked_redis_client(startup_nodes=startup_nodes)
        assert cluster.get_node(host=default_host, port=port_1) is not None \
            and cluster.get_node(host=default_host, port=port_2) is not None

    def test_empty_startup_nodes(self):
        """
        Test that exception is raised when empty providing empty startup_nodes
        """
        with pytest.raises(RedisClusterException) as ex:
            RedisCluster(startup_nodes=[])

        assert str(ex.value).startswith(
            "RedisCluster requires at least one node to discover the "
            "cluster"), str_if_bytes(ex.value)

    def test_from_url(self, r):
        redis_url = "redis://{0}:{1}/0".format(default_host, default_port)
        with patch.object(RedisCluster, 'from_url') as from_url:
            def from_url_mocked(_url, **_kwargs):
                return get_mocked_redis_client(url=_url, **_kwargs)

            from_url.side_effect = from_url_mocked
            cluster = RedisCluster.from_url(redis_url)
        assert cluster.get_node(host=default_host,
                                port=default_port) is not None

    def test_execute_command_errors(self, r):
        """
        Test that if no key is provided then exception should be raised.
        """
        with pytest.raises(RedisClusterException) as ex:
            r.execute_command("GET")
        assert str(ex.value).startswith("No way to dispatch this command to "
                                        "Redis Cluster. Missing key.")

    def test_execute_command_node_flag_primaries(self, r):
        """
        Test command execution with nodes flag PRIMARIES
        """
        primaries = r.get_primaries()
        replicas = r.get_replicas()
        mock_all_nodes_resp(r, 'PONG')
        assert r.ping(RedisCluster.PRIMARIES) is True
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
        mock_all_nodes_resp(r, 'PONG')
        assert r.ping(RedisCluster.REPLICAS) is True
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
        mock_all_nodes_resp(r, 'PONG')
        assert r.ping(RedisCluster.ALL_NODES) is True
        for node in r.get_nodes():
            conn = node.redis_connection.connection
            assert conn.read_response.called is True

    def test_execute_command_node_flag_random(self, r):
        """
        Test command execution with nodes flag RANDOM
        """
        mock_all_nodes_resp(r, 'PONG')
        assert r.ping(RedisCluster.RANDOM) is True
        called_count = 0
        for node in r.get_nodes():
            conn = node.redis_connection.connection
            if conn.read_response.called is True:
                called_count += 1
        assert called_count == 1

    @pytest.mark.filterwarnings("ignore:AskError")
    def test_ask_redirection(self, r):
        """
        Test that the server handles ASK response.

        At first call it should return a ASK ResponseError that will point
        the client to the next server it should talk to.

        Important thing to verify is that it tries to talk to the second node.
        """
        redirect_node = r.get_nodes()[0]
        with patch.object(Redis, 'parse_response') as parse_response:
            def ask_redirect_effect(connection, *args, **options):
                def ok_response(connection, *args, **options):
                    assert connection.host == redirect_node.host
                    assert connection.port == redirect_node.port

                    return "MOCK_OK"

                parse_response.side_effect = ok_response
                raise AskError("12182 {0}:{1}".format(redirect_node.host,
                                                      redirect_node.port))

            parse_response.side_effect = ask_redirect_effect

            assert r.execute_command("SET", "foo", "bar") == "MOCK_OK"

    @pytest.mark.filterwarnings("ignore:MovedError")
    def test_moved_redirection(self, request):
        """
        Test that the client handles MOVED response.
        """
        moved_redirection_helper(request, failover=False)

    @pytest.mark.filterwarnings("ignore:MovedError")
    def test_moved_redirection_after_failover(self, request):
        """
        Test that the client handles MOVED response after a failover.
        """
        moved_redirection_helper(request, failover=True)

    @pytest.mark.filterwarnings("ignore:ClusterDownError")
    def test_refresh_using_specific_nodes(self, request):
        """
        Test making calls on specific nodes when the cluster has failed over to
        another node
        """
        node_7006 = ClusterNode(host=default_host, port=7006,
                                server_type=PRIMARY)
        node_7007 = ClusterNode(host=default_host, port=7007,
                                server_type=PRIMARY)
        with patch.object(Redis, 'parse_response') as parse_response:
            with patch.object(NodesManager, 'initialize', autospec=True) as \
                    initialize:
                with patch.multiple(Connection,
                                    send_command=DEFAULT,
                                    connect=DEFAULT,
                                    can_read=DEFAULT) as mocks:
                    # simulate 7006 as a failed node
                    def parse_response_mock(connection, command_name,
                                            **options):
                        if connection.port == 7006:
                            parse_response.failed_calls += 1
                            raise ClusterDownError(
                                'CLUSTERDOWN The cluster is '
                                'down. Use CLUSTER INFO for '
                                'more information')
                        elif connection.port == 7007:
                            parse_response.successful_calls += 1

                    def initialize_mock(self):
                        # start with all slots mapped to 7006
                        self.nodes_cache = {node_7006.name: node_7006}
                        self.slots_cache = {}

                        for i in range(0, 16383):
                            self.slots_cache[i] = [node_7006]

                        # After the first connection fails, a reinitialize
                        # should follow the cluster to 7007
                        def map_7007(self):
                            self.nodes_cache = {
                                node_7007.name: node_7007}
                            self.slots_cache = {}

                            for i in range(0, 16383):
                                self.slots_cache[i] = [node_7007]

                        # Change initialize side effect for the second call
                        initialize.side_effect = map_7007

                    parse_response.side_effect = parse_response_mock
                    parse_response.successful_calls = 0
                    parse_response.failed_calls = 0
                    initialize.side_effect = initialize_mock
                    mocks['can_read'].return_value = False
                    mocks['send_command'].return_value = "MOCK_OK"
                    mocks['connect'].return_value = None
                    with patch.object(CommandsParser, 'initialize',
                                      autospec=True) as cmd_parser_initialize:

                        def cmd_init_mock(self, r):
                            self.commands = {'get': {'name': 'get', 'arity': 2,
                                                     'flags': ['readonly',
                                                               'fast'],
                                                     'first_key_pos': 1,
                                                     'last_key_pos': 1,
                                                     'step_count': 1}}

                        cmd_parser_initialize.side_effect = cmd_init_mock

                        rc = _get_client(
                            RedisCluster, request, flushdb=False)
                        assert len(rc.get_nodes()) == 1
                        assert rc.get_node(node_name=node_7006.name) is not \
                            None

                        rc.get('foo')

                        # Cluster should now point to 7007, and there should be
                        # one failed and one successful call
                        assert len(rc.get_nodes()) == 1
                        assert rc.get_node(node_name=node_7007.name) is not \
                            None
                        assert rc.get_node(node_name=node_7006.name) is None
                        assert parse_response.failed_calls == 1
                        assert parse_response.successful_calls == 1

    def test_reading_from_replicas_in_round_robin(self):
        with patch.multiple(Connection, send_command=DEFAULT,
                            read_response=DEFAULT, _connect=DEFAULT,
                            can_read=DEFAULT, on_connect=DEFAULT) as mocks:
            with patch.object(Redis, 'parse_response') as parse_response:
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
                mocks['send_command'].return_value = True
                mocks['read_response'].return_value = "OK"
                mocks['_connect'].return_value = True
                mocks['can_read'].return_value = False
                mocks['on_connect'].return_value = True

                # Create a cluster with reading from replications
                read_cluster = get_mocked_redis_client(host=default_host,
                                                       port=default_port,
                                                       read_from_replicas=True)
                assert read_cluster.read_from_replicas is True
                # Check that we read from the slot's nodes in a round robin
                # matter.
                # 'foo' belongs to slot 12182 and the slot's nodes are:
                # [(127.0.0.1,7001,primary), (127.0.0.1,7002,replica)]
                read_cluster.get("foo")
                read_cluster.get("foo")
                read_cluster.get("foo")
                mocks['send_command'].assert_has_calls([call('READONLY')])

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
        assert r.keyslot(u"大奖") == r.keyslot(b"\xe5\xa4\xa7\xe5\xa5\x96")
        assert r.keyslot(1337.1234) == r.keyslot("1337.1234")
        assert r.keyslot(1337) == r.keyslot("1337")
        assert r.keyslot(b"abc") == r.keyslot("abc")

    def test_get_node_name(self):
        assert get_node_name(default_host, default_port) == \
            "{0}:{1}".format(default_host, default_port)

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
        nodes = [node for node in r.nodes_manager.nodes_cache.values()
                 if node.server_type == PRIMARY]

        for node in r.get_primaries():
            assert node in nodes

    @pytest.mark.filterwarnings("ignore:ClusterDownError")
    def test_cluster_down_overreaches_retry_attempts(self):
        """
        When ClusterDownError is thrown, test that we retry executing the
        command as many times as configured in cluster_error_retry_attempts
        and then raise the exception
        """
        with patch.object(RedisCluster, '_execute_command') as execute_command:
            def raise_cluster_down_error(target_node, *args, **kwargs):
                execute_command.failed_calls += 1
                raise ClusterDownError(
                    'CLUSTERDOWN The cluster is down. Use CLUSTER INFO for '
                    'more information')

            execute_command.side_effect = raise_cluster_down_error

            rc = get_mocked_redis_client(host=default_host, port=default_port)

            with pytest.raises(ClusterDownError):
                rc.get("bar")
                assert execute_command.failed_calls == \
                    rc.cluster_error_retry_attempts

    @pytest.mark.filterwarnings("ignore:ConnectionError")
    def test_connection_error_overreaches_retry_attempts(self):
        """
        When ConnectionError is thrown, test that we retry executing the
        command as many times as configured in cluster_error_retry_attempts
        and then raise the exception
        """
        with patch.object(RedisCluster, '_execute_command') as execute_command:
            def raise_conn_error(target_node, *args, **kwargs):
                execute_command.failed_calls += 1
                raise ConnectionError()

            execute_command.side_effect = raise_conn_error

            rc = get_mocked_redis_client(host=default_host, port=default_port)

            with pytest.raises(ConnectionError):
                rc.get("bar")
                assert execute_command.failed_calls == \
                    rc.cluster_error_retry_attempts

    def test_user_on_connect_function(self, request):
        """
        Test support in passing on_connect function by the user
        """

        def on_connect(connection):
            assert connection is not None

        mock = Mock(side_effect=on_connect)

        _get_client(RedisCluster, request, redis_connect_func=mock)
        assert mock.called is True


@skip_if_not_cluster_mode()
class TestClusterRedisCommands:
    def test_case_insensitive_command_names(self, r):
        assert r.cluster_response_callbacks['cluster addslots'] == \
            r.cluster_response_callbacks['CLUSTER ADDSLOTS']

    def test_get_and_set(self, r):
        # get and set can't be tested independently of each other
        assert r.get('a') is None
        byte_string = b'value'
        integer = 5
        unicode_string = chr(3456) + 'abcd' + chr(3421)
        assert r.set('byte_string', byte_string)
        assert r.set('integer', 5)
        assert r.set('unicode_string', unicode_string)
        assert r.get('byte_string') == byte_string
        assert r.get('integer') == str(integer).encode()
        assert r.get('unicode_string').decode('utf-8') == unicode_string

    def test_mget_nonatomic(self, r):
        assert r.mget_nonatomic([]) == []
        assert r.mget_nonatomic(['a', 'b']) == [None, None]
        r['a'] = '1'
        r['b'] = '2'
        r['c'] = '3'

        assert (r.mget_nonatomic('a', 'other', 'b', 'c') ==
                [b'1', None, b'2', b'3'])

    def test_mset_nonatomic(self, r):
        d = {'a': b'1', 'b': b'2', 'c': b'3', 'd': b'4'}
        assert r.mset_nonatomic(d)
        for k, v in d.items():
            assert r[k] == v

    def test_dbsize(self, r):
        d = {'a': b'1', 'b': b'2', 'c': b'3', 'd': b'4'}
        assert r.mset_nonatomic(d)
        assert r.dbsize() == len(d)

    def test_config_set(self, r):
        assert r.config_set('slowlog-log-slower-than', 0)

    def test_client_setname(self, r):
        r.client_setname('redis_py_test')
        res = r.client_getname()
        for client_name in res.values():
            assert client_name == 'redis_py_test'

    def test_exists(self, r):
        d = {'a': b'1', 'b': b'2', 'c': b'3', 'd': b'4'}
        r.mset_nonatomic(d)
        assert r.exists(*d.keys()) == len(d)

    def test_delete(self, r):
        d = {'a': b'1', 'b': b'2', 'c': b'3', 'd': b'4'}
        r.mset_nonatomic(d)
        assert r.delete(*d.keys()) == len(d)
        assert r.delete(*d.keys()) == 0

    def test_touch(self, r):
        d = {'a': b'1', 'b': b'2', 'c': b'3', 'd': b'4'}
        r.mset_nonatomic(d)
        assert r.touch(*d.keys()) == len(d)

    def test_unlink(self, r):
        d = {'a': b'1', 'b': b'2', 'c': b'3', 'd': b'4'}
        r.mset_nonatomic(d)
        assert r.unlink(*d.keys()) == len(d)
        # Unlink is non-blocking so we sleep before
        # verifying the deletion
        sleep(0.1)
        assert r.unlink(*d.keys()) == 0

    def test_pubsub_channels_merge_results(self, r):
        nodes = r.get_nodes()
        channels = []
        i = 0
        for node in nodes:
            channel = "foo{0}".format(i)
            # We will create different pubsub clients where each one is
            # connected to a different node
            p = r.pubsub(node)
            p.subscribe(channel)
            b_channel = channel.encode('utf-8')
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
        result = r.pubsub_channels()
        result.sort()
        assert result == channels

    def test_pubsub_numsub_merge_results(self, r):
        nodes = r.get_nodes()
        channel = "foo"
        b_channel = channel.encode('utf-8')
        for node in nodes:
            # We will create different pubsub clients where each one is
            # connected to a different node
            p = r.pubsub(node)
            p.subscribe(channel)
            # Assert that each node returns that only one client is subscribed
            sub_chann_num = node.redis_connection.pubsub_numsub(channel)
            if sub_chann_num == [(b_channel, 0)]:
                sleep(0.3)
                sub_chann_num = node.redis_connection.pubsub_numsub(channel)
            assert sub_chann_num == [(b_channel, 1)]
        # Assert that the cluster's pubsub_numsub function returns ALL clients
        # subscribed to this channel in the entire cluster
        assert r.pubsub_numsub(channel) == [(b_channel, len(nodes))]

    def test_pubsub_numpat_merge_results(self, r):
        nodes = r.get_nodes()
        pattern = "foo*"
        for node in nodes:
            # We will create different pubsub clients where each one is
            # connected to a different node
            p = r.pubsub(node)
            p.psubscribe(pattern)
            # Assert that each node returns that only one client is subscribed
            sub_num_pat = node.redis_connection.pubsub_numpat()
            if sub_num_pat == 0:
                sleep(0.3)
                sub_num_pat = node.redis_connection.pubsub_numpat()
            assert sub_num_pat == 1
        # Assert that the cluster's pubsub_numsub function returns ALL clients
        # subscribed to this channel in the entire cluster
        assert r.pubsub_numpat() == len(nodes)

    def test_cluster_slots(self, r):
        mock_all_nodes_resp(r, default_cluster_slots)
        cluster_slots = r.cluster_slots()
        assert isinstance(cluster_slots, dict)
        assert len(default_cluster_slots) == len(cluster_slots)
        assert cluster_slots.get((0, 8191)) is not None
        assert cluster_slots.get((0, 8191)).get('primary') == \
            ('127.0.0.1', 7000)

    def test_cluster_addslots(self, r):
        node = r.get_random_node()
        mock_node_resp(node, 'OK')
        assert r.cluster_addslots(node, 1, 2, 3) is True

    def test_cluster_countkeysinslot(self, r):
        node = r.nodes_manager.get_node_from_slot(1)
        mock_node_resp(node, 2)
        assert r.cluster_countkeysinslot(1) == 2

    def test_cluster_count_failure_report(self, r):
        mock_all_nodes_resp(r, 0)
        assert r.cluster_count_failure_report('node_0') == 0

    def test_cluster_delslots(self):
        cluster_slots = [
            [
                0, 8191,
                ['127.0.0.1', 7000, 'node_0'],
            ],
            [
                8192, 16383,
                ['127.0.0.1', 7001, 'node_1'],
            ]
        ]
        r = get_mocked_redis_client(host=default_host, port=default_port,
                                    cluster_slots=cluster_slots)
        mock_all_nodes_resp(r, 'OK')
        node0 = r.get_node(default_host, 7000)
        node1 = r.get_node(default_host, 7001)
        assert r.cluster_delslots(0, 8192) == [True, True]
        assert node0.redis_connection.connection.read_response.called
        assert node1.redis_connection.connection.read_response.called

    def test_cluster_failover(self, r):
        node = r.get_random_node()
        mock_node_resp(node, 'OK')
        assert r.cluster_failover(node) is True
        assert r.cluster_failover(node, 'FORCE') is True
        assert r.cluster_failover(node, 'TAKEOVER') is True
        with pytest.raises(RedisError):
            r.cluster_failover(node, 'FORCT')

    def test_cluster_info(self, r):
        info = r.cluster_info()
        assert isinstance(info, dict)
        assert info['cluster_state'] == 'ok'

    def test_cluster_keyslot(self, r):
        mock_all_nodes_resp(r, 12182)
        assert r.cluster_keyslot('foo') == 12182

    def test_cluster_meet(self, r):
        node = r.get_random_node()
        mock_node_resp(node, 'OK')
        assert r.cluster_meet(node, '127.0.0.1', 6379) is True

    def test_cluster_nodes(self, r):
        response = (
            'c8253bae761cb1ecb2b61857d85dfe455a0fec8b 172.17.0.7:7006 '
            'slave aa90da731f673a99617dfe930306549a09f83a6b 0 '
            '1447836263059 5 connected\n'
            '9bd595fe4821a0e8d6b99d70faa660638a7612b3 172.17.0.7:7008 '
            'master - 0 1447836264065 0 connected\n'
            'aa90da731f673a99617dfe930306549a09f83a6b 172.17.0.7:7003 '
            'myself,master - 0 0 2 connected 5461-10922\n'
            '1df047e5a594f945d82fc140be97a1452bcbf93e 172.17.0.7:7007 '
            'slave 19efe5a631f3296fdf21a5441680f893e8cc96ec 0 '
            '1447836262556 3 connected\n'
            '4ad9a12e63e8f0207025eeba2354bcf4c85e5b22 172.17.0.7:7005 '
            'master - 0 1447836262555 7 connected 0-5460\n'
            '19efe5a631f3296fdf21a5441680f893e8cc96ec 172.17.0.7:7004 '
            'master - 0 1447836263562 3 connected 10923-16383\n'
            'fbb23ed8cfa23f17eaf27ff7d0c410492a1093d6 172.17.0.7:7002 '
            'master,fail - 1447829446956 1447829444948 1 disconnected\n'
        )
        mock_all_nodes_resp(r, response)
        nodes = r.cluster_nodes()
        assert len(nodes) == 7
        assert nodes.get('172.17.0.7:7006') is not None
        assert nodes.get('172.17.0.7:7006').get('node_id') == \
            "c8253bae761cb1ecb2b61857d85dfe455a0fec8b"

    def test_cluster_replicate(self, r):
        node = r.get_random_node()
        all_replicas = r.get_replicas()
        mock_all_nodes_resp(r, 'OK')
        assert r.cluster_replicate(node, 'c8253bae761cb61857d') is True
        results = r.cluster_replicate(all_replicas, 'c8253bae761cb61857d')
        for res in results.values():
            assert res is True

    def test_cluster_reset(self, r):
        node = r.get_random_node()
        all_nodes = r.get_nodes()
        mock_all_nodes_resp(r, 'OK')
        assert r.cluster_reset(node) is True
        assert r.cluster_reset(node, False) is True
        all_results = r.cluster_reset(all_nodes, False)
        for res in all_results.values():
            assert res is True

    def test_cluster_save_config(self, r):
        node = r.get_random_node()
        all_nodes = r.get_nodes()
        mock_all_nodes_resp(r, 'OK')
        assert r.cluster_save_config(node) is True
        all_results = r.cluster_save_config(all_nodes)
        for res in all_results.values():
            assert res is True

    def test_cluster_get_keys_in_slot(self, r):
        response = [b'{foo}1', b'{foo}2']
        node = r.nodes_manager.get_node_from_slot(12182)
        mock_node_resp(node, response)
        keys = r.cluster_get_keys_in_slot(12182, 4)
        assert keys == response

    def test_cluster_set_config_epoch(self, r):
        node = r.get_random_node()
        all_nodes = r.get_nodes()
        mock_all_nodes_resp(r, 'OK')
        assert r.cluster_set_config_epoch(node, 3) is True
        all_results = r.cluster_set_config_epoch(all_nodes, 3)
        for res in all_results.values():
            assert res is True

    def test_cluster_setslot(self, r):
        node = r.get_random_node()
        mock_node_resp(node, 'OK')
        assert r.cluster_setslot(node, 'node_0', 1218, 'IMPORTING') is True
        assert r.cluster_setslot(node, 'node_0', 1218, 'NODE') is True
        assert r.cluster_setslot(node, 'node_0', 1218, 'MIGRATING') is True
        with pytest.raises(RedisError):
            r.cluster_failover(node, 'STABLE')
        with pytest.raises(RedisError):
            r.cluster_failover(node, 'STATE')

    def test_cluster_setslot_stable(self, r):
        node = r.nodes_manager.get_node_from_slot(12182)
        mock_node_resp(node, 'OK')
        assert r.cluster_setslot_stable(12182) is True
        assert node.redis_connection.connection.read_response.called

    def test_cluster_replicas(self, r):
        response = [b'01eca22229cf3c652b6fca0d09ff6941e0d2e3 '
                    b'127.0.0.1:6377@16377 slave '
                    b'52611e796814b78e90ad94be9d769a4f668f9a 0 '
                    b'1634550063436 4 connected',
                    b'r4xfga22229cf3c652b6fca0d09ff69f3e0d4d '
                    b'127.0.0.1:6378@16378 slave '
                    b'52611e796814b78e90ad94be9d769a4f668f9a 0 '
                    b'1634550063436 4 connected']
        mock_all_nodes_resp(r, response)
        replicas = r.cluster_replicas('52611e796814b78e90ad94be9d769a4f668f9a')
        assert replicas.get('127.0.0.1:6377') is not None
        assert replicas.get('127.0.0.1:6378') is not None
        assert replicas.get('127.0.0.1:6378').get('node_id') == \
            'r4xfga22229cf3c652b6fca0d09ff69f3e0d4d'

    def test_readonly(self):
        r = get_mocked_redis_client(host=default_host, port=default_port)
        node = r.get_random_node()
        all_replicas = r.get_replicas()
        mock_all_nodes_resp(r, 'OK')
        assert r.readonly(node) is True
        all_replicas_results = r.readonly()
        for res in all_replicas_results.values():
            assert res is True
        for replica in all_replicas:
            assert replica.redis_connection.connection.read_response.called

    def test_readwrite(self):
        r = get_mocked_redis_client(host=default_host, port=default_port)
        node = r.get_random_node()
        mock_all_nodes_resp(r, 'OK')
        all_replicas = r.get_replicas()
        assert r.readwrite(node) is True
        all_replicas_results = r.readwrite()
        for res in all_replicas_results.values():
            assert res is True
        for replica in all_replicas:
            assert replica.redis_connection.connection.read_response.called

    def test_bgsave(self, r):
        assert r.bgsave()
        sleep(0.3)
        assert r.bgsave(True)

    def test_info(self, r):
        # Map keys to same slot
        r.set('x{1}', 1)
        r.set('y{1}', 2)
        r.set('z{1}', 3)
        # Get node that handles the slot
        slot = r.keyslot('x{1}')
        node = r.nodes_manager.get_node_from_slot(slot)
        # Run info on that node
        info = r.info(target_nodes=node)
        assert isinstance(info, dict)
        assert info['db0']['keys'] == 3

    def test_slowlog_get(self, r, slowlog):
        assert r.slowlog_reset()
        unicode_string = chr(3456) + 'abcd' + chr(3421)
        r.get(unicode_string)

        slot = r.keyslot(unicode_string)
        node = r.nodes_manager.get_node_from_slot(slot)
        slowlog = r.slowlog_get(target_nodes=node)
        assert isinstance(slowlog, list)
        commands = [log['command'] for log in slowlog]

        get_command = b' '.join((b'GET', unicode_string.encode('utf-8')))
        assert get_command in commands
        assert b'SLOWLOG RESET' in commands

        # the order should be ['GET <uni string>', 'SLOWLOG RESET'],
        # but if other clients are executing commands at the same time, there
        # could be commands, before, between, or after, so just check that
        # the two we care about are in the appropriate order.
        assert commands.index(get_command) < commands.index(b'SLOWLOG RESET')

        # make sure other attributes are typed correctly
        assert isinstance(slowlog[0]['start_time'], int)
        assert isinstance(slowlog[0]['duration'], int)

    def test_slowlog_get_limit(self, r, slowlog):
        assert r.slowlog_reset()
        r.get('foo')
        node = r.nodes_manager.get_node_from_slot(key_slot(b'foo'))
        slowlog = r.slowlog_get(1, target_nodes=node)
        assert isinstance(slowlog, list)
        # only one command, based on the number we passed to slowlog_get()
        assert len(slowlog) == 1

    def test_slowlog_length(self, r, slowlog):
        r.get('foo')
        node = r.nodes_manager.get_node_from_slot(key_slot(b'foo'))
        slowlog_len = r.slowlog_len(target_nodes=node)
        assert isinstance(slowlog_len, int)

    def test_time(self, r):
        t = r.time(target_nodes=r.get_primaries()[0])
        assert len(t) == 2
        assert isinstance(t[0], int)
        assert isinstance(t[1], int)

    @skip_if_server_version_lt('4.0.0')
    def test_memory_usage(self, r):
        r.set('foo', 'bar')
        assert isinstance(r.memory_usage('foo'), int)

    @skip_if_server_version_lt('4.0.0')
    def test_memory_malloc_stats(self, r):
        assert r.memory_malloc_stats()

    @skip_if_server_version_lt('4.0.0')
    def test_memory_stats(self, r):
        # put a key into the current db to make sure that "db.<current-db>"
        # has data
        r.set('foo', 'bar')
        node = r.nodes_manager.get_node_from_slot(key_slot(b'foo'))
        stats = r.memory_stats(target_nodes=node)
        assert isinstance(stats, dict)
        for key, value in stats.items():
            if key.startswith('db.'):
                assert isinstance(value, dict)

    @skip_if_server_version_lt('4.0.0')
    def test_memory_help(self, r):
        with pytest.raises(NotImplementedError):
            r.memory_help()

    @skip_if_server_version_lt('4.0.0')
    def test_memory_doctor(self, r):
        with pytest.raises(NotImplementedError):
            r.memory_doctor()

    def test_object(self, r):
        r['a'] = 'foo'
        assert isinstance(r.object('refcount', 'a'), int)
        assert isinstance(r.object('idletime', 'a'), int)
        assert r.object('encoding', 'a') in (b'raw', b'embstr')
        assert r.object('idletime', 'invalid-key') is None

    def test_lastsave(self, r):
        node = r.get_primaries()[0]
        assert isinstance(r.lastsave(target_nodes=node),
                          datetime.datetime)

    def test_echo(self, r):
        node = r.get_primaries()[0]
        assert r.echo('foo bar', node) == b'foo bar'

    @skip_if_server_version_lt('1.0.0')
    def test_debug_segfault(self, r):
        with pytest.raises(NotImplementedError):
            r.debug_segfault()

    def test_config_resetstat(self, r):
        node = r.get_primaries()[0]
        r.ping(target_nodes=node)
        prior_commands_processed = \
            int(r.info(target_nodes=node)['total_commands_processed'])
        assert prior_commands_processed >= 1
        r.config_resetstat(target_nodes=node)
        reset_commands_processed = \
            int(r.info(target_nodes=node)['total_commands_processed'])
        assert reset_commands_processed < prior_commands_processed

    @skip_if_server_version_lt('6.2.0')
    def test_client_trackinginfo(self, r):
        node = r.get_primaries()[0]
        res = r.client_trackinginfo(target_nodes=node)
        assert len(res) > 2
        assert 'prefixes' in res

    @skip_if_server_version_lt('2.9.50')
    def test_client_pause(self, r):
        node = r.get_primaries()[0]
        assert r.client_pause(1, target_nodes=node)
        assert r.client_pause(timeout=1, target_nodes=node)
        with pytest.raises(RedisError):
            r.client_pause(timeout='not an integer', target_nodes=node)

    @skip_if_server_version_lt('6.2.0')
    def test_client_unpause(self, r):
        assert r.client_unpause()

    @skip_if_server_version_lt('5.0.0')
    def test_client_id(self, r):
        node = r.get_primaries()[0]
        assert r.client_id(target_nodes=node) > 0

    @skip_if_server_version_lt('5.0.0')
    def test_client_unblock(self, r):
        node = r.get_primaries()[0]
        myid = r.client_id(target_nodes=node)
        assert not r.client_unblock(myid, target_nodes=node)
        assert not r.client_unblock(myid, error=True, target_nodes=node)
        assert not r.client_unblock(myid, error=False, target_nodes=node)

    @skip_if_server_version_lt('6.0.0')
    def test_client_getredir(self, r):
        node = r.get_primaries()[0]
        assert isinstance(r.client_getredir(target_nodes=node), int)
        assert r.client_getredir(target_nodes=node) == -1

    @skip_if_server_version_lt('6.2.0')
    def test_client_info(self, r):
        node = r.get_primaries()[0]
        info = r.client_info(target_nodes=node)
        assert isinstance(info, dict)
        assert 'addr' in info

    @skip_if_server_version_lt('2.6.9')
    def test_client_kill(self, r, r2):
        node = r.get_primaries()[0]
        r.client_setname('redis-py-c1')
        r2.client_setname('redis-py-c2')
        clients = [client for client in r.client_list()[node.name]
                   if client.get('name') in ['redis-py-c1', 'redis-py-c2']]
        assert len(clients) == 2
        clients_by_name = dict([(client.get('name'), client)
                                for client in clients])

        client_addr = clients_by_name['redis-py-c2'].get('addr')
        assert r.client_kill(client_addr, target_nodes=node) is True

        clients = [client for client in r.client_list()[node.name]
                   if client.get('name') in ['redis-py-c1', 'redis-py-c2']]
        assert len(clients) == 1
        assert clients[0].get('name') == 'redis-py-c1'


@skip_if_not_cluster_mode()
class TestNodesManager:
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
            slot_2: [node_4, node_5]
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
            [0, 5459, ['127.0.0.1', 7000], ['127.0.0.1', 7003]],
            [5461, 10922, ['127.0.0.1', 7001],
             ['127.0.0.1', 7004]],
            [10923, 16383, ['127.0.0.1', 7002],
             ['127.0.0.1', 7005]],
        ]
        with pytest.raises(RedisClusterException) as ex:
            get_mocked_redis_client(host=default_host, port=default_port,
                                    cluster_slots=cluster_slots)
        assert str(ex.value).startswith(
            "All slots are not covered after query all startup_nodes.")

    def test_init_slots_cache_not_require_full_coverage_error(self):
        """
        When require_full_coverage is set to False and not all slots are
        covered, if one of the nodes has 'cluster-require_full_coverage'
        config set to 'yes' the cluster initialization should fail
        """
        # Missing slot 5460
        cluster_slots = [
            [0, 5459, ['127.0.0.1', 7000], ['127.0.0.1', 7003]],
            [5461, 10922, ['127.0.0.1', 7001],
             ['127.0.0.1', 7004]],
            [10923, 16383, ['127.0.0.1', 7002],
             ['127.0.0.1', 7005]],
        ]

        with pytest.raises(RedisClusterException):
            get_mocked_redis_client(host=default_host, port=default_port,
                                    cluster_slots=cluster_slots,
                                    require_full_coverage=False,
                                    coverage_result='yes')

    def test_init_slots_cache_not_require_full_coverage_success(self):
        """
        When require_full_coverage is set to False and not all slots are
        covered, if all of the nodes has 'cluster-require_full_coverage'
        config set to 'no' the cluster initialization should succeed
        """
        # Missing slot 5460
        cluster_slots = [
            [0, 5459, ['127.0.0.1', 7000], ['127.0.0.1', 7003]],
            [5461, 10922, ['127.0.0.1', 7001],
             ['127.0.0.1', 7004]],
            [10923, 16383, ['127.0.0.1', 7002],
             ['127.0.0.1', 7005]],
        ]

        rc = get_mocked_redis_client(host=default_host, port=default_port,
                                     cluster_slots=cluster_slots,
                                     require_full_coverage=False,
                                     coverage_result='no')

        assert 5460 not in rc.nodes_manager.slots_cache

    def test_init_slots_cache_not_require_full_coverage_skips_check(self):
        """
        Test that when require_full_coverage is set to False and
        skip_full_coverage_check is set to true, the cluster initialization
        succeed without checking the nodes' Redis configurations
        """
        # Missing slot 5460
        cluster_slots = [
            [0, 5459, ['127.0.0.1', 7000], ['127.0.0.1', 7003]],
            [5461, 10922, ['127.0.0.1', 7001],
             ['127.0.0.1', 7004]],
            [10923, 16383, ['127.0.0.1', 7002],
             ['127.0.0.1', 7005]],
        ]

        with patch.object(NodesManager,
                          'cluster_require_full_coverage') as conf_check_mock:
            rc = get_mocked_redis_client(host=default_host, port=default_port,
                                         cluster_slots=cluster_slots,
                                         require_full_coverage=False,
                                         skip_full_coverage_check=True,
                                         coverage_result='no')

            assert conf_check_mock.called is False
            assert 5460 not in rc.nodes_manager.slots_cache

    def test_init_slots_cache(self):
        """
        Test that slots cache can in initialized and all slots are covered
        """
        good_slots_resp = [
            [0, 5460, ['127.0.0.1', 7000], ['127.0.0.2', 7003]],
            [5461, 10922, ['127.0.0.1', 7001], ['127.0.0.2', 7004]],
            [10923, 16383, ['127.0.0.1', 7002], ['127.0.0.2', 7005]],
        ]

        rc = get_mocked_redis_client(host=default_host, port=default_port,
                                     cluster_slots=good_slots_resp)
        n_manager = rc.nodes_manager
        assert len(n_manager.slots_cache) == REDIS_CLUSTER_HASH_SLOTS
        for slot_info in good_slots_resp:
            all_hosts = ['127.0.0.1', '127.0.0.2']
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
        with patch.object(NodesManager,
                          'create_redis_node') as create_redis_node:
            def create_mocked_redis_node(host, port, **kwargs):
                """
                Helper function to return custom slots cache data from
                different redis nodes
                """
                if port == 7000:
                    result = [
                        [
                            0,
                            5460,
                            ['127.0.0.1', 7000],
                            ['127.0.0.1', 7003],
                        ],
                        [
                            5461,
                            10922,
                            ['127.0.0.1', 7001],
                            ['127.0.0.1', 7004],
                        ],
                    ]

                elif port == 7001:
                    result = [
                        [
                            0,
                            5460,
                            ['127.0.0.1', 7001],
                            ['127.0.0.1', 7003],
                        ],
                        [
                            5461,
                            10922,
                            ['127.0.0.1', 7000],
                            ['127.0.0.1', 7004],
                        ],
                    ]
                else:
                    result = []

                r_node = Redis(
                    host=host,
                    port=port
                )

                orig_execute_command = r_node.execute_command

                def execute_command(*args, **kwargs):
                    if args[0] == 'CLUSTER SLOTS':
                        return result
                    elif args[1] == 'cluster-require-full-coverage':
                        return {'cluster-require-full-coverage': 'yes'}
                    else:
                        return orig_execute_command(*args, **kwargs)

                r_node.execute_command = execute_command
                return r_node

            create_redis_node.side_effect = create_mocked_redis_node

            with pytest.raises(RedisClusterException) as ex:
                node_1 = ClusterNode('127.0.0.1', 7000)
                node_2 = ClusterNode('127.0.0.1', 7001)
                RedisCluster(startup_nodes=[node_1, node_2])
            assert str(ex.value).startswith(
                "startup_nodes could not agree on a valid slots cache"), str(
                ex.value)

    def test_cluster_one_instance(self):
        """
        If the cluster exists of only 1 node then there is some hacks that must
        be validated they work.
        """
        node = ClusterNode(default_host, default_port)
        cluster_slots = [[0, 16383, ['', default_port]]]
        rc = get_mocked_redis_client(startup_nodes=[node],
                                     cluster_slots=cluster_slots)

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
        with patch.object(NodesManager,
                          'create_redis_node') as create_redis_node:
            def create_mocked_redis_node(host, port, **kwargs):
                if port == 7000:
                    raise ConnectionError('mock connection error for 7000')

                r_node = Redis(host=host, port=port, decode_responses=True)

                def execute_command(*args, **kwargs):
                    if args[0] == 'CLUSTER SLOTS':
                        return [
                            [
                                0, 8191,
                                ['127.0.0.1', 7001, 'node_1'],
                            ],
                            [
                                8192, 16383,
                                ['127.0.0.1', 7002, 'node_2'],
                            ]
                        ]
                    elif args[1] == 'cluster-require-full-coverage':
                        return {'cluster-require-full-coverage': 'yes'}

                r_node.execute_command = execute_command

                return r_node

            create_redis_node.side_effect = create_mocked_redis_node

            node_1 = ClusterNode('127.0.0.1', 7000)
            node_2 = ClusterNode('127.0.0.1', 7001)

            # If all startup nodes fail to connect, connection error should be
            # thrown
            with pytest.raises(RedisClusterException) as e:
                RedisCluster(startup_nodes=[node_1])
            assert 'Redis Cluster cannot be connected' in str(e.value)

            with patch.object(CommandsParser, 'initialize',
                              autospec=True) as cmd_parser_initialize:

                def cmd_init_mock(self, r):
                    self.commands = {'get': {'name': 'get', 'arity': 2,
                                             'flags': ['readonly',
                                                       'fast'],
                                             'first_key_pos': 1,
                                             'last_key_pos': 1,
                                             'step_count': 1}}

                cmd_parser_initialize.side_effect = cmd_init_mock
                # When at least one startup node is reachable, the cluster
                # initialization should succeeds
                rc = RedisCluster(startup_nodes=[node_1, node_2])
                assert rc.get_node(host=default_host, port=7001) is not None
                assert rc.get_node(host=default_host, port=7002) is not None
