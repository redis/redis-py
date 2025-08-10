import binascii
import datetime
import re
import threading
import time
from asyncio import CancelledError
from string import ascii_letters
from unittest import mock
from unittest.mock import patch

import pytest

import redis
from redis import exceptions
from redis.client import parse_info

from .conftest import (
    _get_client,
    skip_if_redis_enterprise,
    skip_if_server_version_gte,
    skip_if_server_version_lt,
    skip_unless_arch_bits,
)


@pytest.fixture()
def slowlog(request, r):
    current_config = r.config_get()
    old_slower_than_value = current_config["slowlog-log-slower-than"]
    old_max_legnth_value = current_config["slowlog-max-len"]

    def cleanup():
        r.config_set("slowlog-log-slower-than", old_slower_than_value)
        r.config_set("slowlog-max-len", old_max_legnth_value)

    request.addfinalizer(cleanup)

    r.config_set("slowlog-log-slower-than", 0)
    r.config_set("slowlog-max-len", 128)


def redis_server_time(client):
    seconds, milliseconds = client.time()
    timestamp = float(f"{seconds}.{milliseconds}")
    return datetime.datetime.fromtimestamp(timestamp)


def get_stream_message(client, stream, message_id):
    "Fetch a stream message and format it as a (message_id, fields) pair"
    response = client.xrange(stream, min=message_id, max=message_id)
    assert len(response) == 1
    return response[0]


# RESPONSE CALLBACKS
@pytest.mark.onlynoncluster
class TestResponseCallbacks:
    "Tests for the response callback system"

    def test_response_callbacks(self, r):
        assert r.response_callbacks == redis.Redis.RESPONSE_CALLBACKS
        assert id(r.response_callbacks) != id(redis.Redis.RESPONSE_CALLBACKS)
        r.set_response_callback("GET", lambda x: "static")
        r["a"] = "foo"
        assert r["a"] == "static"

    def test_case_insensitive_command_names(self, r):
        assert r.response_callbacks["del"] == r.response_callbacks["DEL"]


class TestRedisCommands:
    @skip_if_redis_enterprise()
    def test_auth(self, r, request):
        # first, test for default user (`username` is supposed to be optional)
        default_username = "default"
        temp_pass = "temp_pass"
        r.config_set("requirepass", temp_pass)

        assert r.auth(temp_pass, default_username) is True
        assert r.auth(temp_pass) is True

        # test for other users
        username = "redis-py-auth"

        def teardown():
            try:
                r.auth(temp_pass)
            except exceptions.ResponseError:
                r.auth("default", "")
            r.config_set("requirepass", "")
            r.acl_deluser(username)

        request.addfinalizer(teardown)

        assert r.acl_setuser(
            username, enabled=True, passwords=["+strong_password"], commands=["+acl"]
        )

        assert r.auth(username=username, password="strong_password") is True

        with pytest.raises(exceptions.ResponseError):
            r.auth(username=username, password="wrong_password")

    def test_command_on_invalid_key_type(self, r):
        r.lpush("a", "1")
        with pytest.raises(redis.ResponseError):
            r["a"]

    # SERVER INFORMATION
    @skip_if_server_version_lt("6.0.0")
    def test_acl_cat_no_category(self, r):
        categories = r.acl_cat()
        assert isinstance(categories, list)
        assert "read" in categories

    @skip_if_server_version_lt("6.0.0")
    def test_acl_cat_with_category(self, r):
        commands = r.acl_cat("read")
        assert isinstance(commands, list)
        assert "get" in commands

    @skip_if_server_version_lt("7.0.0")
    @skip_if_redis_enterprise()
    def test_acl_dryrun(self, r, request):
        username = "redis-py-user"

        def teardown():
            r.acl_deluser(username)

        request.addfinalizer(teardown)

        r.acl_setuser(username, keys=["*"], commands=["+set"])
        assert r.acl_dryrun(username, "set", "key", "value") == b"OK"
        assert r.acl_dryrun(username, "get", "key").startswith(
            b"This user has no permissions to run the"
        )

    @skip_if_server_version_lt("6.0.0")
    @skip_if_redis_enterprise()
    def test_acl_deluser(self, r, request):
        username = "redis-py-user"

        def teardown():
            r.acl_deluser(username)

        request.addfinalizer(teardown)

        assert r.acl_deluser(username) == 0
        assert r.acl_setuser(username, enabled=False, reset=True)
        assert r.acl_deluser(username) == 1

        # now, a group of users
        users = [f"bogususer_{r}" for r in range(0, 5)]
        for u in users:
            r.acl_setuser(u, enabled=False, reset=True)
        assert r.acl_deluser(*users) > 1
        assert r.acl_getuser(users[0]) is None
        assert r.acl_getuser(users[1]) is None
        assert r.acl_getuser(users[2]) is None
        assert r.acl_getuser(users[3]) is None
        assert r.acl_getuser(users[4]) is None

    @skip_if_server_version_lt("6.0.0")
    @skip_if_redis_enterprise()
    def test_acl_genpass(self, r):
        password = r.acl_genpass()
        assert isinstance(password, str)

        with pytest.raises(exceptions.DataError):
            r.acl_genpass("value")
            r.acl_genpass(-5)
            r.acl_genpass(5555)

        r.acl_genpass(555)
        assert isinstance(password, str)

    @skip_if_server_version_lt("7.0.0")
    @skip_if_redis_enterprise()
    def test_acl_getuser_setuser(self, r, request):
        username = "redis-py-user"

        def teardown():
            r.acl_deluser(username)

        request.addfinalizer(teardown)

        # test enabled=False
        assert r.acl_setuser(username, enabled=False, reset=True)
        acl = r.acl_getuser(username)
        assert acl["categories"] == ["-@all"]
        assert acl["commands"] == []
        assert acl["keys"] == []
        assert acl["passwords"] == []
        assert "off" in acl["flags"]
        assert acl["enabled"] is False

        # test nopass=True
        assert r.acl_setuser(username, enabled=True, reset=True, nopass=True)
        acl = r.acl_getuser(username)
        assert acl["categories"] == ["-@all"]
        assert acl["commands"] == []
        assert acl["keys"] == []
        assert acl["passwords"] == []
        assert "on" in acl["flags"]
        assert "nopass" in acl["flags"]
        assert acl["enabled"] is True

        # test all args
        assert r.acl_setuser(
            username,
            enabled=True,
            reset=True,
            passwords=["+pass1", "+pass2"],
            categories=["+set", "+@hash", "-geo"],
            commands=["+get", "+mget", "-hset"],
            keys=["cache:*", "objects:*"],
        )
        acl = r.acl_getuser(username)
        assert set(acl["categories"]) == {"-@all", "+@set", "+@hash"}
        assert set(acl["commands"]) == {"+get", "+mget", "-hset"}
        assert acl["enabled"] is True
        assert "on" in acl["flags"]
        assert set(acl["keys"]) == {"~cache:*", "~objects:*"}
        assert len(acl["passwords"]) == 2

        # test reset=False keeps existing ACL and applies new ACL on top
        assert r.acl_setuser(
            username,
            enabled=True,
            reset=True,
            passwords=["+pass1"],
            categories=["+@set"],
            commands=["+get"],
            keys=["cache:*"],
        )
        assert r.acl_setuser(
            username,
            enabled=True,
            passwords=["+pass2"],
            categories=["+@hash"],
            commands=["+mget"],
            keys=["objects:*"],
        )
        acl = r.acl_getuser(username)
        assert set(acl["categories"]) == {"-@all", "+@set", "+@hash"}
        assert set(acl["commands"]) == {"+get", "+mget"}
        assert acl["enabled"] is True
        assert "on" in acl["flags"]
        assert set(acl["keys"]) == {"~cache:*", "~objects:*"}
        assert len(acl["passwords"]) == 2

        # test removal of passwords
        assert r.acl_setuser(
            username, enabled=True, reset=True, passwords=["+pass1", "+pass2"]
        )
        assert len(r.acl_getuser(username)["passwords"]) == 2
        assert r.acl_setuser(username, enabled=True, passwords=["-pass2"])
        assert len(r.acl_getuser(username)["passwords"]) == 1

        # Resets and tests that hashed passwords are set properly.
        hashed_password = (
            "5e884898da28047151d0e56f8dc629" "2773603d0d6aabbdd62a11ef721d1542d8"
        )
        assert r.acl_setuser(
            username, enabled=True, reset=True, hashed_passwords=["+" + hashed_password]
        )
        acl = r.acl_getuser(username)
        assert acl["passwords"] == [hashed_password]

        # test removal of hashed passwords
        assert r.acl_setuser(
            username,
            enabled=True,
            reset=True,
            hashed_passwords=["+" + hashed_password],
            passwords=["+pass1"],
        )
        assert len(r.acl_getuser(username)["passwords"]) == 2
        assert r.acl_setuser(
            username, enabled=True, hashed_passwords=["-" + hashed_password]
        )
        assert len(r.acl_getuser(username)["passwords"]) == 1

        # test selectors
        assert r.acl_setuser(
            username,
            enabled=True,
            reset=True,
            passwords=["+pass1", "+pass2"],
            categories=["+set", "+@hash", "-geo"],
            commands=["+get", "+mget", "-hset"],
            keys=["cache:*", "objects:*"],
            channels=["message:*"],
            selectors=[("+set", "%W~app*")],
        )
        acl = r.acl_getuser(username)
        assert set(acl["categories"]) == {"-@all", "+@set", "+@hash"}
        assert set(acl["commands"]) == {"+get", "+mget", "-hset"}
        assert acl["enabled"] is True
        assert "on" in acl["flags"]
        assert set(acl["keys"]) == {"~cache:*", "~objects:*"}
        assert len(acl["passwords"]) == 2
        assert set(acl["channels"]) == {"&message:*"}
        assert acl["selectors"] == [
            ["commands", "-@all +set", "keys", "%W~app*", "channels", ""]
        ]

    @skip_if_server_version_lt("6.0.0")
    def test_acl_help(self, r):
        res = r.acl_help()
        assert isinstance(res, list)
        assert len(res) != 0

    @skip_if_server_version_lt("6.0.0")
    @skip_if_redis_enterprise()
    def test_acl_list(self, r, request):
        username = "redis-py-user"

        def teardown():
            r.acl_deluser(username)

        request.addfinalizer(teardown)

        assert r.acl_setuser(username, enabled=False, reset=True)
        users = r.acl_list()
        assert len(users) == 2

    @skip_if_server_version_lt("6.0.0")
    @skip_if_redis_enterprise()
    @pytest.mark.onlynoncluster
    def test_acl_log(self, r, request):
        username = "redis-py-user"

        def teardown():
            r.acl_deluser(username)

        request.addfinalizer(teardown)
        r.acl_setuser(
            username,
            enabled=True,
            reset=True,
            commands=["+get", "+set", "+select"],
            keys=["cache:*"],
            nopass=True,
        )
        r.acl_log_reset()

        user_client = _get_client(
            redis.Redis, request, flushdb=False, username=username
        )

        # Valid operation and key
        assert user_client.set("cache:0", 1)
        assert user_client.get("cache:0") == b"1"

        # Invalid key
        with pytest.raises(exceptions.NoPermissionError):
            user_client.get("violated_cache:0")

        # Invalid operation
        with pytest.raises(exceptions.NoPermissionError):
            user_client.hset("cache:0", "hkey", "hval")

        assert isinstance(r.acl_log(), list)
        assert len(r.acl_log()) == 2
        assert len(r.acl_log(count=1)) == 1
        assert isinstance(r.acl_log()[0], dict)
        assert "client-info" in r.acl_log(count=1)[0]
        assert r.acl_log_reset()

    @skip_if_server_version_lt("6.0.0")
    @skip_if_redis_enterprise()
    def test_acl_setuser_categories_without_prefix_fails(self, r, request):
        username = "redis-py-user"

        def teardown():
            r.acl_deluser(username)

        request.addfinalizer(teardown)

        with pytest.raises(exceptions.DataError):
            r.acl_setuser(username, categories=["list"])

    @skip_if_server_version_lt("6.0.0")
    @skip_if_redis_enterprise()
    def test_acl_setuser_commands_without_prefix_fails(self, r, request):
        username = "redis-py-user"

        def teardown():
            r.acl_deluser(username)

        request.addfinalizer(teardown)

        with pytest.raises(exceptions.DataError):
            r.acl_setuser(username, commands=["get"])

    @skip_if_server_version_lt("6.0.0")
    @skip_if_redis_enterprise()
    def test_acl_setuser_add_passwords_and_nopass_fails(self, r, request):
        username = "redis-py-user"

        def teardown():
            r.acl_deluser(username)

        request.addfinalizer(teardown)

        with pytest.raises(exceptions.DataError):
            r.acl_setuser(username, passwords="+mypass", nopass=True)

    @skip_if_server_version_lt("6.0.0")
    def test_acl_users(self, r):
        users = r.acl_users()
        assert isinstance(users, list)
        assert len(users) > 0

    @skip_if_server_version_lt("6.0.0")
    def test_acl_whoami(self, r):
        username = r.acl_whoami()
        assert isinstance(username, str)

    @pytest.mark.onlynoncluster
    def test_client_list(self, r):
        clients = r.client_list()
        assert isinstance(clients[0], dict)
        assert "addr" in clients[0]

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    def test_client_info(self, r):
        info = r.client_info()
        assert isinstance(info, dict)
        assert "addr" in info

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("5.0.0")
    def test_client_list_types_not_replica(self, r):
        with pytest.raises(exceptions.RedisError):
            r.client_list(_type="not a client type")
        for client_type in ["normal", "master", "pubsub"]:
            clients = r.client_list(_type=client_type)
            assert isinstance(clients, list)

    @skip_if_redis_enterprise()
    def test_client_list_replica(self, r):
        clients = r.client_list(_type="replica")
        assert isinstance(clients, list)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    def test_client_list_client_id(self, r, request):
        clients = r.client_list()
        clients = r.client_list(client_id=[clients[0]["id"]])
        assert len(clients) == 1
        assert "addr" in clients[0]

        # testing multiple client ids
        _get_client(redis.Redis, request, flushdb=False)
        _get_client(redis.Redis, request, flushdb=False)
        _get_client(redis.Redis, request, flushdb=False)
        clients_listed = r.client_list(client_id=clients[:-1])
        assert len(clients_listed) > 1

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("5.0.0")
    def test_client_id(self, r):
        assert r.client_id() > 0

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    @skip_if_redis_enterprise()
    def test_client_trackinginfo(self, r):
        res = r.client_trackinginfo()
        assert len(res) > 2
        assert "prefixes" in res

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.0.0")
    @skip_if_redis_enterprise()
    def test_client_tracking(self, r, r2):

        # simple case
        assert r.client_tracking_on()
        assert r.client_tracking_off()

        # id based
        client_id = r.client_id()
        assert r.client_tracking_on(client_id)
        assert r.client_tracking_off(client_id)

        # id exists
        client_id = r2.client_id()
        assert r.client_tracking_on(client_id)
        assert r2.client_tracking_off(client_id)

        # now with some prefixes
        with pytest.raises(exceptions.DataError):
            assert r.client_tracking_on(prefix=["foo", "bar", "blee"])

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("5.0.0")
    def test_client_unblock(self, r):
        myid = r.client_id()
        assert not r.client_unblock(myid)
        assert not r.client_unblock(myid, error=True)
        assert not r.client_unblock(myid, error=False)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.6.9")
    def test_client_getname(self, r):
        assert r.client_getname() is None

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.6.9")
    def test_client_setname(self, r):
        assert r.client_setname("redis_py_test")
        assert r.client_getname() == "redis_py_test"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.6.9")
    def test_client_kill(self, r, r2):
        r.client_setname("redis-py-c1")
        r2.client_setname("redis-py-c2")
        clients = [
            client
            for client in r.client_list()
            if client.get("name") in ["redis-py-c1", "redis-py-c2"]
        ]
        assert len(clients) == 2

        clients_by_name = {client.get("name"): client for client in clients}

        client_addr = clients_by_name["redis-py-c2"].get("addr")
        assert r.client_kill(client_addr) is True

        clients = [
            client
            for client in r.client_list()
            if client.get("name") in ["redis-py-c1", "redis-py-c2"]
        ]
        assert len(clients) == 1
        assert clients[0].get("name") == "redis-py-c1"

    @skip_if_server_version_lt("2.8.12")
    def test_client_kill_filter_invalid_params(self, r):
        # empty
        with pytest.raises(exceptions.DataError):
            r.client_kill_filter()

        # invalid skipme
        with pytest.raises(exceptions.DataError):
            r.client_kill_filter(skipme="yeah")

        # invalid type
        with pytest.raises(exceptions.DataError):
            r.client_kill_filter(_type="caster")

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.12")
    def test_client_kill_filter_by_id(self, r, r2):
        r.client_setname("redis-py-c1")
        r2.client_setname("redis-py-c2")
        clients = [
            client
            for client in r.client_list()
            if client.get("name") in ["redis-py-c1", "redis-py-c2"]
        ]
        assert len(clients) == 2

        clients_by_name = {client.get("name"): client for client in clients}

        client_2_id = clients_by_name["redis-py-c2"].get("id")
        resp = r.client_kill_filter(_id=client_2_id)
        assert resp == 1

        clients = [
            client
            for client in r.client_list()
            if client.get("name") in ["redis-py-c1", "redis-py-c2"]
        ]
        assert len(clients) == 1
        assert clients[0].get("name") == "redis-py-c1"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.12")
    def test_client_kill_filter_by_addr(self, r, r2):
        r.client_setname("redis-py-c1")
        r2.client_setname("redis-py-c2")
        clients = [
            client
            for client in r.client_list()
            if client.get("name") in ["redis-py-c1", "redis-py-c2"]
        ]
        assert len(clients) == 2

        clients_by_name = {client.get("name"): client for client in clients}

        client_2_addr = clients_by_name["redis-py-c2"].get("addr")
        resp = r.client_kill_filter(addr=client_2_addr)
        assert resp == 1

        clients = [
            client
            for client in r.client_list()
            if client.get("name") in ["redis-py-c1", "redis-py-c2"]
        ]
        assert len(clients) == 1
        assert clients[0].get("name") == "redis-py-c1"

    @skip_if_server_version_lt("2.6.9")
    def test_client_list_after_client_setname(self, r):
        r.client_setname("redis_py_test")
        clients = r.client_list()
        # we don't know which client ours will be
        assert "redis_py_test" in [c["name"] for c in clients]

    @skip_if_server_version_lt("6.2.0")
    def test_client_kill_filter_by_laddr(self, r, r2):
        r.client_setname("redis-py-c1")
        r2.client_setname("redis-py-c2")
        clients = [
            client
            for client in r.client_list()
            if client.get("name") in ["redis-py-c1", "redis-py-c2"]
        ]
        assert len(clients) == 2

        clients_by_name = {client.get("name"): client for client in clients}

        client_2_addr = clients_by_name["redis-py-c2"].get("laddr")
        assert r.client_kill_filter(laddr=client_2_addr)

    @skip_if_server_version_lt("6.0.0")
    @skip_if_redis_enterprise()
    def test_client_kill_filter_by_user(self, r, request):
        killuser = "user_to_kill"
        r.acl_setuser(
            killuser,
            enabled=True,
            reset=True,
            commands=["+get", "+set", "+select", "+cluster", "+command", "+info"],
            keys=["cache:*"],
            nopass=True,
        )
        _get_client(redis.Redis, request, flushdb=False, username=killuser)
        r.client_kill_filter(user=killuser)
        clients = r.client_list()
        for c in clients:
            assert c["user"] != killuser
        r.acl_deluser(killuser)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.9.50")
    @skip_if_redis_enterprise()
    def test_client_pause(self, r):
        assert r.client_pause(1)
        assert r.client_pause(timeout=1)
        with pytest.raises(exceptions.RedisError):
            r.client_pause(timeout="not an integer")

    @skip_if_server_version_lt("6.2.0")
    @skip_if_redis_enterprise()
    def test_client_pause_all(self, r, r2):
        assert r.client_pause(1, all=False)
        assert r2.set("foo", "bar")
        assert r2.get("foo") == b"bar"
        assert r.get("foo") == b"bar"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    @skip_if_redis_enterprise()
    def test_client_unpause(self, r):
        assert r.client_unpause() == b"OK"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_client_no_evict(self, r):
        assert r.client_no_evict("ON")
        with pytest.raises(TypeError):
            r.client_no_evict()

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("3.2.0")
    def test_client_reply(self, r, r_timeout):
        assert r_timeout.client_reply("ON") == b"OK"
        with pytest.raises(exceptions.TimeoutError):
            r_timeout.client_reply("OFF")

            r_timeout.client_reply("SKIP")

        assert r_timeout.set("foo", "bar")

        # validate it was set
        assert r.get("foo") == b"bar"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.0.0")
    @skip_if_redis_enterprise()
    def test_client_getredir(self, r):
        assert isinstance(r.client_getredir(), int)
        assert r.client_getredir() == -1

    @skip_if_server_version_lt("6.0.0")
    def test_hello_notI_implemented(self, r):
        with pytest.raises(NotImplementedError):
            r.hello()

    def test_config_get(self, r):
        data = r.config_get()
        assert len(data.keys()) > 10
        # # assert 'maxmemory' in data
        # assert data['maxmemory'].isdigit()

    @skip_if_server_version_lt("7.0.0")
    def test_config_get_multi_params(self, r: redis.Redis):
        res = r.config_get("*max-*-entries*", "maxmemory")
        assert "maxmemory" in res
        assert "hash-max-listpack-entries" in res

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_config_resetstat(self, r):
        r.ping()
        prior_commands_processed = int(r.info()["total_commands_processed"])
        assert prior_commands_processed >= 1
        r.config_resetstat()
        reset_commands_processed = int(r.info()["total_commands_processed"])
        assert reset_commands_processed < prior_commands_processed

    @skip_if_redis_enterprise()
    def test_config_set(self, r):
        r.config_set("timeout", 70)
        assert r.config_get()["timeout"] == "70"
        assert r.config_set("timeout", 0)
        assert r.config_get()["timeout"] == "0"

    @skip_if_server_version_lt("7.0.0")
    @skip_if_redis_enterprise()
    def test_config_set_multi_params(self, r: redis.Redis):
        r.config_set("timeout", 70, "maxmemory", 100)
        assert r.config_get()["timeout"] == "70"
        assert r.config_get()["maxmemory"] == "100"
        assert r.config_set("timeout", 0, "maxmemory", 0)
        assert r.config_get()["timeout"] == "0"
        assert r.config_get()["maxmemory"] == "0"

    @skip_if_server_version_lt("6.0.0")
    @skip_if_redis_enterprise()
    def test_failover(self, r):
        with pytest.raises(NotImplementedError):
            r.failover()

    @pytest.mark.onlynoncluster
    def test_dbsize(self, r):
        r["a"] = "foo"
        r["b"] = "bar"
        assert r.dbsize() == 2

    @pytest.mark.onlynoncluster
    def test_echo(self, r):
        assert r.echo("foo bar") == b"foo bar"

    @pytest.mark.onlynoncluster
    def test_info(self, r):
        r["a"] = "foo"
        r["b"] = "bar"
        info = r.info()
        assert isinstance(info, dict)
        assert "arch_bits" in info.keys()
        assert "redis_version" in info.keys()

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_info_multi_sections(self, r):
        res = r.info("clients", "server")
        assert isinstance(res, dict)
        assert "redis_version" in res
        assert "connected_clients" in res

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_lastsave(self, r):
        assert isinstance(r.lastsave(), datetime.datetime)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("5.0.0")
    def test_lolwut(self, r):
        lolwut = r.lolwut().decode("utf-8")
        assert "Redis ver." in lolwut

        lolwut = r.lolwut(5, 6, 7, 8).decode("utf-8")
        assert "Redis ver." in lolwut

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    @skip_if_redis_enterprise()
    def test_reset(self, r):
        assert r.reset() == "RESET"

    def test_object(self, r):
        r["a"] = "foo"
        assert isinstance(r.object("refcount", "a"), int)
        assert isinstance(r.object("idletime", "a"), int)
        assert r.object("encoding", "a") in (b"raw", b"embstr")
        assert r.object("idletime", "invalid-key") is None

    def test_ping(self, r):
        assert r.ping()

    @pytest.mark.onlynoncluster
    def test_quit(self, r):
        assert r.quit()

    @skip_if_server_version_lt("2.8.12")
    @skip_if_redis_enterprise()
    @pytest.mark.onlynoncluster
    def test_role(self, r):
        assert r.role()[0] == b"master"
        assert isinstance(r.role()[1], int)
        assert isinstance(r.role()[2], list)

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_select(self, r):
        assert r.select(5)
        assert r.select(2)
        assert r.select(9)

    @pytest.mark.onlynoncluster
    def test_slowlog_get(self, r, slowlog):
        assert r.slowlog_reset()
        unicode_string = chr(3456) + "abcd" + chr(3421)
        r.get(unicode_string)
        slowlog = r.slowlog_get()
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

        # Mock result if we didn't get slowlog complexity info.
        if "complexity" not in slowlog[0]:
            # monkey patch parse_response()
            COMPLEXITY_STATEMENT = "Complexity info: N:4712,M:3788"
            old_parse_response = r.parse_response

            def parse_response(connection, command_name, **options):
                if command_name != "SLOWLOG GET":
                    return old_parse_response(connection, command_name, **options)
                responses = connection.read_response()
                for response in responses:
                    # Complexity info stored as fourth item in list
                    response.insert(3, COMPLEXITY_STATEMENT)
                return r.response_callbacks[command_name](responses, **options)

            r.parse_response = parse_response

            # test
            slowlog = r.slowlog_get()
            assert isinstance(slowlog, list)
            commands = [log["command"] for log in slowlog]
            assert get_command in commands
            idx = commands.index(get_command)
            assert slowlog[idx]["complexity"] == COMPLEXITY_STATEMENT

            # tear down monkeypatch
            r.parse_response = old_parse_response

    @pytest.mark.onlynoncluster
    def test_slowlog_get_limit(self, r, slowlog):
        assert r.slowlog_reset()
        r.get("foo")
        slowlog = r.slowlog_get(1)
        assert isinstance(slowlog, list)
        # only one command, based on the number we passed to slowlog_get()
        assert len(slowlog) == 1

    @pytest.mark.onlynoncluster
    def test_slowlog_length(self, r, slowlog):
        r.get("foo")
        assert isinstance(r.slowlog_len(), int)

    @skip_if_server_version_lt("2.6.0")
    def test_time(self, r):
        t = r.time()
        assert len(t) == 2
        assert isinstance(t[0], int)
        assert isinstance(t[1], int)

    @skip_if_redis_enterprise()
    def test_bgsave(self, r):
        assert r.bgsave()
        time.sleep(0.3)
        assert r.bgsave(True)

    # BASIC KEY COMMANDS
    def test_append(self, r):
        assert r.append("a", "a1") == 2
        assert r["a"] == b"a1"
        assert r.append("a", "a2") == 4
        assert r["a"] == b"a1a2"

    @skip_if_server_version_lt("2.6.0")
    def test_bitcount(self, r):
        r.setbit("a", 5, True)
        assert r.bitcount("a") == 1
        r.setbit("a", 6, True)
        assert r.bitcount("a") == 2
        r.setbit("a", 5, False)
        assert r.bitcount("a") == 1
        r.setbit("a", 9, True)
        r.setbit("a", 17, True)
        r.setbit("a", 25, True)
        r.setbit("a", 33, True)
        assert r.bitcount("a") == 5
        assert r.bitcount("a", 0, -1) == 5
        assert r.bitcount("a", 2, 3) == 2
        assert r.bitcount("a", 2, -1) == 3
        assert r.bitcount("a", -2, -1) == 2
        assert r.bitcount("a", 1, 1) == 1

    @skip_if_server_version_lt("7.0.0")
    def test_bitcount_mode(self, r):
        r.set("mykey", "foobar")
        assert r.bitcount("mykey") == 26
        assert r.bitcount("mykey", 1, 1, "byte") == 6
        assert r.bitcount("mykey", 5, 30, "bit") == 17
        with pytest.raises(redis.ResponseError):
            assert r.bitcount("mykey", 5, 30, "but")

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.6.0")
    def test_bitop_not_empty_string(self, r):
        r["a"] = ""
        r.bitop("not", "r", "a")
        assert r.get("r") is None

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.6.0")
    def test_bitop_not(self, r):
        test_str = b"\xAA\x00\xFF\x55"
        correct = ~0xAA00FF55 & 0xFFFFFFFF
        r["a"] = test_str
        r.bitop("not", "r", "a")
        assert int(binascii.hexlify(r["r"]), 16) == correct

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.6.0")
    def test_bitop_not_in_place(self, r):
        test_str = b"\xAA\x00\xFF\x55"
        correct = ~0xAA00FF55 & 0xFFFFFFFF
        r["a"] = test_str
        r.bitop("not", "a", "a")
        assert int(binascii.hexlify(r["a"]), 16) == correct

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.6.0")
    def test_bitop_single_string(self, r):
        test_str = b"\x01\x02\xFF"
        r["a"] = test_str
        r.bitop("and", "res1", "a")
        r.bitop("or", "res2", "a")
        r.bitop("xor", "res3", "a")
        assert r["res1"] == test_str
        assert r["res2"] == test_str
        assert r["res3"] == test_str

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.6.0")
    def test_bitop_string_operands(self, r):
        r["a"] = b"\x01\x02\xFF\xFF"
        r["b"] = b"\x01\x02\xFF"
        r.bitop("and", "res1", "a", "b")
        r.bitop("or", "res2", "a", "b")
        r.bitop("xor", "res3", "a", "b")
        assert int(binascii.hexlify(r["res1"]), 16) == 0x0102FF00
        assert int(binascii.hexlify(r["res2"]), 16) == 0x0102FFFF
        assert int(binascii.hexlify(r["res3"]), 16) == 0x000000FF

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.7")
    def test_bitpos(self, r):
        key = "key:bitpos"
        r.set(key, b"\xff\xf0\x00")
        assert r.bitpos(key, 0) == 12
        assert r.bitpos(key, 0, 2, -1) == 16
        assert r.bitpos(key, 0, -2, -1) == 12
        r.set(key, b"\x00\xff\xf0")
        assert r.bitpos(key, 1, 0) == 8
        assert r.bitpos(key, 1, 1) == 8
        r.set(key, b"\x00\x00\x00")
        assert r.bitpos(key, 1) == -1

    @skip_if_server_version_lt("2.8.7")
    def test_bitpos_wrong_arguments(self, r):
        key = "key:bitpos:wrong:args"
        r.set(key, b"\xff\xf0\x00")
        with pytest.raises(exceptions.RedisError):
            r.bitpos(key, 0, end=1) == 12
        with pytest.raises(exceptions.RedisError):
            r.bitpos(key, 7) == 12

    @skip_if_server_version_lt("7.0.0")
    def test_bitpos_mode(self, r):
        r.set("mykey", b"\x00\xff\xf0")
        assert r.bitpos("mykey", 1, 0) == 8
        assert r.bitpos("mykey", 1, 2, -1, "byte") == 16
        assert r.bitpos("mykey", 0, 7, 15, "bit") == 7
        with pytest.raises(redis.ResponseError):
            r.bitpos("mykey", 1, 7, 15, "bite")

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    def test_copy(self, r):
        assert r.copy("a", "b") == 0
        r.set("a", "foo")
        assert r.copy("a", "b") == 1
        assert r.get("a") == b"foo"
        assert r.get("b") == b"foo"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    def test_copy_and_replace(self, r):
        r.set("a", "foo1")
        r.set("b", "foo2")
        assert r.copy("a", "b") == 0
        assert r.copy("a", "b", replace=True) == 1

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    def test_copy_to_another_database(self, request):
        r0 = _get_client(redis.Redis, request, db=0)
        r1 = _get_client(redis.Redis, request, db=1)
        r0.set("a", "foo")
        assert r0.copy("a", "b", destination_db=1) == 1
        assert r1.get("b") == b"foo"

    def test_decr(self, r):
        assert r.decr("a") == -1
        assert r["a"] == b"-1"
        assert r.decr("a") == -2
        assert r["a"] == b"-2"
        assert r.decr("a", amount=5) == -7
        assert r["a"] == b"-7"

    def test_decrby(self, r):
        assert r.decrby("a", amount=2) == -2
        assert r.decrby("a", amount=3) == -5
        assert r["a"] == b"-5"

    def test_delete(self, r):
        assert r.delete("a") == 0
        r["a"] = "foo"
        assert r.delete("a") == 1

    def test_delete_with_multiple_keys(self, r):
        r["a"] = "foo"
        r["b"] = "bar"
        assert r.delete("a", "b") == 2
        assert r.get("a") is None
        assert r.get("b") is None

    def test_delitem(self, r):
        r["a"] = "foo"
        del r["a"]
        assert r.get("a") is None

    @skip_if_server_version_lt("4.0.0")
    def test_unlink(self, r):
        assert r.unlink("a") == 0
        r["a"] = "foo"
        assert r.unlink("a") == 1
        assert r.get("a") is None

    @skip_if_server_version_lt("4.0.0")
    def test_unlink_with_multiple_keys(self, r):
        r["a"] = "foo"
        r["b"] = "bar"
        assert r.unlink("a", "b") == 2
        assert r.get("a") is None
        assert r.get("b") is None

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_lcs(self, r):
        r.mset({"foo": "ohmytext", "bar": "mynewtext"})
        assert r.lcs("foo", "bar") == b"mytext"
        assert r.lcs("foo", "bar", len=True) == 6
        result = [b"matches", [[[4, 7], [5, 8]]], b"len", 6]
        assert r.lcs("foo", "bar", idx=True, minmatchlen=3) == result
        with pytest.raises(redis.ResponseError):
            assert r.lcs("foo", "bar", len=True, idx=True)

    @skip_if_server_version_lt("2.6.0")
    def test_dump_and_restore(self, r):
        r["a"] = "foo"
        dumped = r.dump("a")
        del r["a"]
        r.restore("a", 0, dumped)
        assert r["a"] == b"foo"

    @skip_if_server_version_lt("3.0.0")
    def test_dump_and_restore_and_replace(self, r):
        r["a"] = "bar"
        dumped = r.dump("a")
        with pytest.raises(redis.ResponseError):
            r.restore("a", 0, dumped)

        r.restore("a", 0, dumped, replace=True)
        assert r["a"] == b"bar"

    @skip_if_server_version_lt("5.0.0")
    def test_dump_and_restore_absttl(self, r):
        r["a"] = "foo"
        dumped = r.dump("a")
        del r["a"]
        ttl = int(
            (redis_server_time(r) + datetime.timedelta(minutes=1)).timestamp() * 1000
        )
        r.restore("a", ttl, dumped, absttl=True)
        assert r["a"] == b"foo"
        assert 0 < r.ttl("a") <= 61

    def test_exists(self, r):
        assert r.exists("a") == 0
        r["a"] = "foo"
        r["b"] = "bar"
        assert r.exists("a") == 1
        assert r.exists("a", "b") == 2

    def test_exists_contains(self, r):
        assert "a" not in r
        r["a"] = "foo"
        assert "a" in r

    def test_expire(self, r):
        assert r.expire("a", 10) is False
        r["a"] = "foo"
        assert r.expire("a", 10) is True
        assert 0 < r.ttl("a") <= 10
        assert r.persist("a")
        assert r.ttl("a") == -1

    @skip_if_server_version_lt("7.0.0")
    def test_expire_option_nx(self, r):
        r.set("key", "val")
        assert r.expire("key", 100, nx=True) == 1
        assert r.expire("key", 500, nx=True) == 0

    @skip_if_server_version_lt("7.0.0")
    def test_expire_option_xx(self, r):
        r.set("key", "val")
        assert r.expire("key", 100, xx=True) == 0
        assert r.expire("key", 100)
        assert r.expire("key", 500, xx=True) == 1

    @skip_if_server_version_lt("7.0.0")
    def test_expire_option_gt(self, r):
        r.set("key", "val", 100)
        assert r.expire("key", 50, gt=True) == 0
        assert r.expire("key", 500, gt=True) == 1

    @skip_if_server_version_lt("7.0.0")
    def test_expire_option_lt(self, r):
        r.set("key", "val", 100)
        assert r.expire("key", 50, lt=True) == 1
        assert r.expire("key", 150, lt=True) == 0

    def test_expireat_datetime(self, r):
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        r["a"] = "foo"
        assert r.expireat("a", expire_at) is True
        assert 0 < r.ttl("a") <= 61

    def test_expireat_no_key(self, r):
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        assert r.expireat("a", expire_at) is False

    def test_expireat_unixtime(self, r):
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        r["a"] = "foo"
        expire_at_seconds = int(time.mktime(expire_at.timetuple()))
        assert r.expireat("a", expire_at_seconds) is True
        assert 0 < r.ttl("a") <= 61

    @skip_if_server_version_lt("7.0.0")
    def test_expiretime(self, r):
        r.set("a", "foo")
        r.expireat("a", 33177117420)
        assert r.expiretime("a") == 33177117420

    @skip_if_server_version_lt("7.0.0")
    def test_expireat_option_nx(self, r):
        assert r.set("key", "val") is True
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        assert r.expireat("key", expire_at, nx=True) is True
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=2)
        assert r.expireat("key", expire_at, nx=True) is False

    @skip_if_server_version_lt("7.0.0")
    def test_expireat_option_xx(self, r):
        assert r.set("key", "val") is True
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        assert r.expireat("key", expire_at, xx=True) is False
        assert r.expireat("key", expire_at) is True
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=2)
        assert r.expireat("key", expire_at, xx=True) is True

    @skip_if_server_version_lt("7.0.0")
    def test_expireat_option_gt(self, r):
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=2)
        assert r.set("key", "val") is True
        assert r.expireat("key", expire_at) is True
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        assert r.expireat("key", expire_at, gt=True) is False
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=3)
        assert r.expireat("key", expire_at, gt=True) is True

    @skip_if_server_version_lt("7.0.0")
    def test_expireat_option_lt(self, r):
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=2)
        assert r.set("key", "val") is True
        assert r.expireat("key", expire_at) is True
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=3)
        assert r.expireat("key", expire_at, lt=True) is False
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        assert r.expireat("key", expire_at, lt=True) is True

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

    @skip_if_server_version_lt("6.2.0")
    def test_getdel(self, r):
        assert r.getdel("a") is None
        r.set("a", 1)
        assert r.getdel("a") == b"1"
        assert r.getdel("a") is None

    @skip_if_server_version_lt("6.2.0")
    def test_getex(self, r):
        r.set("a", 1)
        assert r.getex("a") == b"1"
        assert r.ttl("a") == -1
        assert r.getex("a", ex=60) == b"1"
        assert r.ttl("a") == 60
        assert r.getex("a", px=6000) == b"1"
        assert r.ttl("a") == 6
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        assert r.getex("a", pxat=expire_at) == b"1"
        assert r.ttl("a") <= 61
        assert r.getex("a", persist=True) == b"1"
        assert r.ttl("a") == -1

    def test_getitem_and_setitem(self, r):
        r["a"] = "bar"
        assert r["a"] == b"bar"

    def test_getitem_raises_keyerror_for_missing_key(self, r):
        with pytest.raises(KeyError):
            r["a"]

    def test_getitem_does_not_raise_keyerror_for_empty_string(self, r):
        r["a"] = b""
        assert r["a"] == b""

    def test_get_set_bit(self, r):
        # no value
        assert not r.getbit("a", 5)
        # set bit 5
        assert not r.setbit("a", 5, True)
        assert r.getbit("a", 5)
        # unset bit 4
        assert not r.setbit("a", 4, False)
        assert not r.getbit("a", 4)
        # set bit 4
        assert not r.setbit("a", 4, True)
        assert r.getbit("a", 4)
        # set bit 5 again
        assert r.setbit("a", 5, True)
        assert r.getbit("a", 5)

    def test_getrange(self, r):
        r["a"] = "foo"
        assert r.getrange("a", 0, 0) == b"f"
        assert r.getrange("a", 0, 2) == b"foo"
        assert r.getrange("a", 3, 4) == b""

    def test_getset(self, r):
        assert r.getset("a", "foo") is None
        assert r.getset("a", "bar") == b"foo"
        assert r.get("a") == b"bar"

    def test_incr(self, r):
        assert r.incr("a") == 1
        assert r["a"] == b"1"
        assert r.incr("a") == 2
        assert r["a"] == b"2"
        assert r.incr("a", amount=5) == 7
        assert r["a"] == b"7"

    def test_incrby(self, r):
        assert r.incrby("a") == 1
        assert r.incrby("a", 4) == 5
        assert r["a"] == b"5"

    @skip_if_server_version_lt("2.6.0")
    def test_incrbyfloat(self, r):
        assert r.incrbyfloat("a") == 1.0
        assert r["a"] == b"1"
        assert r.incrbyfloat("a", 1.1) == 2.1
        assert float(r["a"]) == float(2.1)

    @pytest.mark.onlynoncluster
    def test_keys(self, r):
        assert r.keys() == []
        keys_with_underscores = {b"test_a", b"test_b"}
        keys = keys_with_underscores.union({b"testc"})
        for key in keys:
            r[key] = 1
        assert set(r.keys(pattern="test_*")) == keys_with_underscores
        assert set(r.keys(pattern="test*")) == keys

    @pytest.mark.onlynoncluster
    def test_mget(self, r):
        assert r.mget([]) == []
        assert r.mget(["a", "b"]) == [None, None]
        r["a"] = "1"
        r["b"] = "2"
        r["c"] = "3"
        assert r.mget("a", "other", "b", "c") == [b"1", None, b"2", b"3"]

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    def test_lmove(self, r):
        r.rpush("a", "one", "two", "three", "four")
        assert r.lmove("a", "b")
        assert r.lmove("a", "b", "right", "left")

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    def test_blmove(self, r):
        r.rpush("a", "one", "two", "three", "four")
        assert r.blmove("a", "b", 5)
        assert r.blmove("a", "b", 1, "RIGHT", "LEFT")

    @pytest.mark.onlynoncluster
    def test_mset(self, r):
        d = {"a": b"1", "b": b"2", "c": b"3"}
        assert r.mset(d)
        for k, v in d.items():
            assert r[k] == v

    @pytest.mark.onlynoncluster
    def test_msetnx(self, r):
        d = {"a": b"1", "b": b"2", "c": b"3"}
        assert r.msetnx(d)
        d2 = {"a": b"x", "d": b"4"}
        assert not r.msetnx(d2)
        for k, v in d.items():
            assert r[k] == v
        assert r.get("d") is None

    @skip_if_server_version_lt("2.6.0")
    def test_pexpire(self, r):
        assert r.pexpire("a", 60000) is False
        r["a"] = "foo"
        assert r.pexpire("a", 60000) is True
        assert 0 < r.pttl("a") <= 60000
        assert r.persist("a")
        assert r.pttl("a") == -1

    @skip_if_server_version_lt("7.0.0")
    def test_pexpire_option_nx(self, r):
        assert r.set("key", "val") is True
        assert r.pexpire("key", 60000, nx=True) is True
        assert r.pexpire("key", 60000, nx=True) is False

    @skip_if_server_version_lt("7.0.0")
    def test_pexpire_option_xx(self, r):
        assert r.set("key", "val") is True
        assert r.pexpire("key", 60000, xx=True) is False
        assert r.pexpire("key", 60000) is True
        assert r.pexpire("key", 70000, xx=True) is True

    @skip_if_server_version_lt("7.0.0")
    def test_pexpire_option_gt(self, r):
        assert r.set("key", "val") is True
        assert r.pexpire("key", 60000) is True
        assert r.pexpire("key", 70000, gt=True) is True
        assert r.pexpire("key", 50000, gt=True) is False

    @skip_if_server_version_lt("7.0.0")
    def test_pexpire_option_lt(self, r):
        assert r.set("key", "val") is True
        assert r.pexpire("key", 60000) is True
        assert r.pexpire("key", 50000, lt=True) is True
        assert r.pexpire("key", 70000, lt=True) is False

    @skip_if_server_version_lt("2.6.0")
    def test_pexpireat_datetime(self, r):
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        r["a"] = "foo"
        assert r.pexpireat("a", expire_at) is True
        assert 0 < r.pttl("a") <= 61000

    @skip_if_server_version_lt("2.6.0")
    def test_pexpireat_no_key(self, r):
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        assert r.pexpireat("a", expire_at) is False

    @skip_if_server_version_lt("2.6.0")
    def test_pexpireat_unixtime(self, r):
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        r["a"] = "foo"
        expire_at_seconds = int(time.mktime(expire_at.timetuple())) * 1000
        assert r.pexpireat("a", expire_at_seconds) is True
        assert 0 < r.pttl("a") <= 61000

    @skip_if_server_version_lt("7.0.0")
    def test_pexpireat_option_nx(self, r):
        assert r.set("key", "val") is True
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        assert r.pexpireat("key", expire_at, nx=True) is True
        assert r.pexpireat("key", expire_at, nx=True) is False

    @skip_if_server_version_lt("7.0.0")
    def test_pexpireat_option_xx(self, r):
        assert r.set("key", "val") is True
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        assert r.pexpireat("key", expire_at, xx=True) is False
        assert r.pexpireat("key", expire_at) is True
        assert r.pexpireat("key", expire_at, xx=True) is True

    @skip_if_server_version_lt("7.0.0")
    def test_pexpireat_option_gt(self, r):
        assert r.set("key", "val") is True
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=2)
        assert r.pexpireat("key", expire_at) is True
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        assert r.pexpireat("key", expire_at, gt=True) is False
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=3)
        assert r.pexpireat("key", expire_at, gt=True) is True

    @skip_if_server_version_lt("7.0.0")
    def test_pexpireat_option_lt(self, r):
        assert r.set("key", "val") is True
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=2)
        assert r.pexpireat("key", expire_at) is True
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=3)
        assert r.pexpireat("key", expire_at, lt=True) is False
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        assert r.pexpireat("key", expire_at, lt=True) is True

    @skip_if_server_version_lt("7.0.0")
    def test_pexpiretime(self, r):
        r.set("a", "foo")
        r.pexpireat("a", 33177117420000)
        assert r.pexpiretime("a") == 33177117420000

    @skip_if_server_version_lt("2.6.0")
    def test_psetex(self, r):
        assert r.psetex("a", 1000, "value")
        assert r["a"] == b"value"
        assert 0 < r.pttl("a") <= 1000

    @skip_if_server_version_lt("2.6.0")
    def test_psetex_timedelta(self, r):
        expire_at = datetime.timedelta(milliseconds=1000)
        assert r.psetex("a", expire_at, "value")
        assert r["a"] == b"value"
        assert 0 < r.pttl("a") <= 1000

    @skip_if_server_version_lt("2.6.0")
    def test_pttl(self, r):
        assert r.pexpire("a", 10000) is False
        r["a"] = "1"
        assert r.pexpire("a", 10000) is True
        assert 0 < r.pttl("a") <= 10000
        assert r.persist("a")
        assert r.pttl("a") == -1

    @skip_if_server_version_lt("2.8.0")
    def test_pttl_no_key(self, r):
        "PTTL on servers 2.8 and after return -2 when the key doesn't exist"
        assert r.pttl("a") == -2

    @skip_if_server_version_lt("6.2.0")
    def test_hrandfield(self, r):
        assert r.hrandfield("key") is None
        r.hset("key", mapping={"a": 1, "b": 2, "c": 3, "d": 4, "e": 5})
        assert r.hrandfield("key") is not None
        assert len(r.hrandfield("key", 2)) == 2
        # with values
        assert len(r.hrandfield("key", 2, True)) == 4
        # without duplications
        assert len(r.hrandfield("key", 10)) == 5
        # with duplications
        assert len(r.hrandfield("key", -10)) == 10

    @pytest.mark.onlynoncluster
    def test_randomkey(self, r):
        assert r.randomkey() is None
        for key in ("a", "b", "c"):
            r[key] = 1
        assert r.randomkey() in (b"a", b"b", b"c")

    @pytest.mark.onlynoncluster
    def test_rename(self, r):
        r["a"] = "1"
        assert r.rename("a", "b")
        assert r.get("a") is None
        assert r["b"] == b"1"

    @pytest.mark.onlynoncluster
    def test_renamenx(self, r):
        r["a"] = "1"
        r["b"] = "2"
        assert not r.renamenx("a", "b")
        assert r["a"] == b"1"
        assert r["b"] == b"2"

    @skip_if_server_version_lt("2.6.0")
    def test_set_nx(self, r):
        assert r.set("a", "1", nx=True)
        assert not r.set("a", "2", nx=True)
        assert r["a"] == b"1"

    @skip_if_server_version_lt("2.6.0")
    def test_set_xx(self, r):
        assert not r.set("a", "1", xx=True)
        assert r.get("a") is None
        r["a"] = "bar"
        assert r.set("a", "2", xx=True)
        assert r.get("a") == b"2"

    @skip_if_server_version_lt("2.6.0")
    def test_set_px(self, r):
        assert r.set("a", "1", px=10000)
        assert r["a"] == b"1"
        assert 0 < r.pttl("a") <= 10000
        assert 0 < r.ttl("a") <= 10
        with pytest.raises(exceptions.DataError):
            assert r.set("a", "1", px=10.0)

    @skip_if_server_version_lt("2.6.0")
    def test_set_px_timedelta(self, r):
        expire_at = datetime.timedelta(milliseconds=1000)
        assert r.set("a", "1", px=expire_at)
        assert 0 < r.pttl("a") <= 1000
        assert 0 < r.ttl("a") <= 1

    @skip_if_server_version_lt("2.6.0")
    def test_set_ex(self, r):
        assert r.set("a", "1", ex=10)
        assert 0 < r.ttl("a") <= 10
        with pytest.raises(exceptions.DataError):
            assert r.set("a", "1", ex=10.0)

    @skip_if_server_version_lt("2.6.0")
    def test_set_ex_timedelta(self, r):
        expire_at = datetime.timedelta(seconds=60)
        assert r.set("a", "1", ex=expire_at)
        assert 0 < r.ttl("a") <= 60

    @skip_if_server_version_lt("6.2.0")
    def test_set_exat_timedelta(self, r):
        expire_at = redis_server_time(r) + datetime.timedelta(seconds=10)
        assert r.set("a", "1", exat=expire_at)
        assert 0 < r.ttl("a") <= 10

    @skip_if_server_version_lt("6.2.0")
    def test_set_pxat_timedelta(self, r):
        expire_at = redis_server_time(r) + datetime.timedelta(seconds=50)
        assert r.set("a", "1", pxat=expire_at)
        assert 0 < r.ttl("a") <= 100

    @skip_if_server_version_lt("2.6.0")
    def test_set_multipleoptions(self, r):
        r["a"] = "val"
        assert r.set("a", "1", xx=True, px=10000)
        assert 0 < r.ttl("a") <= 10

    @skip_if_server_version_lt("6.0.0")
    def test_set_keepttl(self, r):
        r["a"] = "val"
        assert r.set("a", "1", xx=True, px=10000)
        assert 0 < r.ttl("a") <= 10
        r.set("a", "2", keepttl=True)
        assert r.get("a") == b"2"
        assert 0 < r.ttl("a") <= 10

    @skip_if_server_version_lt("6.2.0")
    def test_set_get(self, r):
        assert r.set("a", "True", get=True) is None
        assert r.set("a", "True", get=True) == b"True"
        assert r.set("a", "foo") is True
        assert r.set("a", "bar", get=True) == b"foo"
        assert r.get("a") == b"bar"

    def test_setex(self, r):
        assert r.setex("a", 60, "1")
        assert r["a"] == b"1"
        assert 0 < r.ttl("a") <= 60

    def test_setnx(self, r):
        assert r.setnx("a", "1")
        assert r["a"] == b"1"
        assert not r.setnx("a", "2")
        assert r["a"] == b"1"

    def test_setrange(self, r):
        assert r.setrange("a", 5, "foo") == 8
        assert r["a"] == b"\0\0\0\0\0foo"
        r["a"] = "abcdefghijh"
        assert r.setrange("a", 6, "12345") == 11
        assert r["a"] == b"abcdef12345"

    @skip_if_server_version_lt("6.0.0")
    @skip_if_server_version_gte("7.0.0")
    @skip_if_redis_enterprise()
    def test_stralgo_lcs(self, r):
        key1 = "{foo}key1"
        key2 = "{foo}key2"
        value1 = "ohmytext"
        value2 = "mynewtext"
        res = "mytext"

        if skip_if_redis_enterprise().args[0] is True:
            with pytest.raises(redis.exceptions.ResponseError):
                assert r.stralgo("LCS", value1, value2) == res
            return

        # test LCS of strings
        assert r.stralgo("LCS", value1, value2) == res
        # test using keys
        r.mset({key1: value1, key2: value2})
        assert r.stralgo("LCS", key1, key2, specific_argument="keys") == res
        # test other labels
        assert r.stralgo("LCS", value1, value2, len=True) == len(res)
        assert r.stralgo("LCS", value1, value2, idx=True) == {
            "len": len(res),
            "matches": [[(4, 7), (5, 8)], [(2, 3), (0, 1)]],
        }
        assert r.stralgo("LCS", value1, value2, idx=True, withmatchlen=True) == {
            "len": len(res),
            "matches": [[4, (4, 7), (5, 8)], [2, (2, 3), (0, 1)]],
        }
        assert r.stralgo(
            "LCS", value1, value2, idx=True, minmatchlen=4, withmatchlen=True
        ) == {"len": len(res), "matches": [[4, (4, 7), (5, 8)]]}

    @skip_if_server_version_lt("6.0.0")
    @skip_if_server_version_gte("7.0.0")
    @skip_if_redis_enterprise()
    def test_stralgo_negative(self, r):
        with pytest.raises(exceptions.DataError):
            r.stralgo("ISSUB", "value1", "value2")
        with pytest.raises(exceptions.DataError):
            r.stralgo("LCS", "value1", "value2", len=True, idx=True)
        with pytest.raises(exceptions.DataError):
            r.stralgo("LCS", "value1", "value2", specific_argument="INT")
        with pytest.raises(ValueError):
            r.stralgo("LCS", "value1", "value2", idx=True, minmatchlen="one")

    def test_strlen(self, r):
        r["a"] = "foo"
        assert r.strlen("a") == 3

    def test_substr(self, r):
        r["a"] = "0123456789"

        if skip_if_redis_enterprise().args[0] is True:
            with pytest.raises(redis.exceptions.ResponseError):
                assert r.substr("a", 0) == b"0123456789"
            return

        assert r.substr("a", 0) == b"0123456789"
        assert r.substr("a", 2) == b"23456789"
        assert r.substr("a", 3, 5) == b"345"
        assert r.substr("a", 3, -2) == b"345678"

    def test_ttl(self, r):
        r["a"] = "1"
        assert r.expire("a", 10)
        assert 0 < r.ttl("a") <= 10
        assert r.persist("a")
        assert r.ttl("a") == -1

    @skip_if_server_version_lt("2.8.0")
    def test_ttl_nokey(self, r):
        "TTL on servers 2.8 and after return -2 when the key doesn't exist"
        assert r.ttl("a") == -2

    def test_type(self, r):
        assert r.type("a") == b"none"
        r["a"] = "1"
        assert r.type("a") == b"string"
        del r["a"]
        r.lpush("a", "1")
        assert r.type("a") == b"list"
        del r["a"]
        r.sadd("a", "1")
        assert r.type("a") == b"set"
        del r["a"]
        r.zadd("a", {"1": 1})
        assert r.type("a") == b"zset"

    # LIST COMMANDS
    @pytest.mark.onlynoncluster
    def test_blpop(self, r):
        r.rpush("a", "1", "2")
        r.rpush("b", "3", "4")
        assert r.blpop(["b", "a"], timeout=1) == (b"b", b"3")
        assert r.blpop(["b", "a"], timeout=1) == (b"b", b"4")
        assert r.blpop(["b", "a"], timeout=1) == (b"a", b"1")
        assert r.blpop(["b", "a"], timeout=1) == (b"a", b"2")
        assert r.blpop(["b", "a"], timeout=1) is None
        r.rpush("c", "1")
        assert r.blpop("c", timeout=1) == (b"c", b"1")

    @pytest.mark.onlynoncluster
    def test_brpop(self, r):
        r.rpush("a", "1", "2")
        r.rpush("b", "3", "4")
        assert r.brpop(["b", "a"], timeout=1) == (b"b", b"4")
        assert r.brpop(["b", "a"], timeout=1) == (b"b", b"3")
        assert r.brpop(["b", "a"], timeout=1) == (b"a", b"2")
        assert r.brpop(["b", "a"], timeout=1) == (b"a", b"1")
        assert r.brpop(["b", "a"], timeout=1) is None
        r.rpush("c", "1")
        assert r.brpop("c", timeout=1) == (b"c", b"1")

    @pytest.mark.onlynoncluster
    def test_brpoplpush(self, r):
        r.rpush("a", "1", "2")
        r.rpush("b", "3", "4")
        assert r.brpoplpush("a", "b") == b"2"
        assert r.brpoplpush("a", "b") == b"1"
        assert r.brpoplpush("a", "b", timeout=1) is None
        assert r.lrange("a", 0, -1) == []
        assert r.lrange("b", 0, -1) == [b"1", b"2", b"3", b"4"]

    @pytest.mark.onlynoncluster
    def test_brpoplpush_empty_string(self, r):
        r.rpush("a", "")
        assert r.brpoplpush("a", "b") == b""

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_blmpop(self, r):
        r.rpush("a", "1", "2", "3", "4", "5")
        res = [b"a", [b"1", b"2"]]
        assert r.blmpop(1, "2", "b", "a", direction="LEFT", count=2) == res
        with pytest.raises(TypeError):
            r.blmpop(1, "2", "b", "a", count=2)
        r.rpush("b", "6", "7", "8", "9")
        assert r.blmpop(0, "2", "b", "a", direction="LEFT") == [b"b", [b"6"]]
        assert r.blmpop(1, "2", "foo", "bar", direction="RIGHT") is None

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_lmpop(self, r):
        r.rpush("foo", "1", "2", "3", "4", "5")
        result = [b"foo", [b"1", b"2"]]
        assert r.lmpop("2", "bar", "foo", direction="LEFT", count=2) == result
        with pytest.raises(redis.ResponseError):
            r.lmpop("2", "bar", "foo", direction="up", count=2)
        r.rpush("bar", "a", "b", "c", "d")
        assert r.lmpop("2", "bar", "foo", direction="LEFT") == [b"bar", [b"a"]]

    def test_lindex(self, r):
        r.rpush("a", "1", "2", "3")
        assert r.lindex("a", "0") == b"1"
        assert r.lindex("a", "1") == b"2"
        assert r.lindex("a", "2") == b"3"

    def test_linsert(self, r):
        r.rpush("a", "1", "2", "3")
        assert r.linsert("a", "after", "2", "2.5") == 4
        assert r.lrange("a", 0, -1) == [b"1", b"2", b"2.5", b"3"]
        assert r.linsert("a", "before", "2", "1.5") == 5
        assert r.lrange("a", 0, -1) == [b"1", b"1.5", b"2", b"2.5", b"3"]

    def test_llen(self, r):
        r.rpush("a", "1", "2", "3")
        assert r.llen("a") == 3

    def test_lpop(self, r):
        r.rpush("a", "1", "2", "3")
        assert r.lpop("a") == b"1"
        assert r.lpop("a") == b"2"
        assert r.lpop("a") == b"3"
        assert r.lpop("a") is None

    @skip_if_server_version_lt("6.2.0")
    def test_lpop_count(self, r):
        r.rpush("a", "1", "2", "3")
        assert r.lpop("a", 2) == [b"1", b"2"]
        assert r.lpop("a", 1) == [b"3"]
        assert r.lpop("a") is None
        assert r.lpop("a", 3) is None

    def test_lpush(self, r):
        assert r.lpush("a", "1") == 1
        assert r.lpush("a", "2") == 2
        assert r.lpush("a", "3", "4") == 4
        assert r.lrange("a", 0, -1) == [b"4", b"3", b"2", b"1"]

    def test_lpushx(self, r):
        assert r.lpushx("a", "1") == 0
        assert r.lrange("a", 0, -1) == []
        r.rpush("a", "1", "2", "3")
        assert r.lpushx("a", "4") == 4
        assert r.lrange("a", 0, -1) == [b"4", b"1", b"2", b"3"]

    @skip_if_server_version_lt("4.0.0")
    def test_lpushx_with_list(self, r):
        # now with a list
        r.lpush("somekey", "a")
        r.lpush("somekey", "b")
        assert r.lpushx("somekey", "foo", "asdasd", 55, "asdasdas") == 6
        res = r.lrange("somekey", 0, -1)
        assert res == [b"asdasdas", b"55", b"asdasd", b"foo", b"b", b"a"]

    def test_lrange(self, r):
        r.rpush("a", "1", "2", "3", "4", "5")
        assert r.lrange("a", 0, 2) == [b"1", b"2", b"3"]
        assert r.lrange("a", 2, 10) == [b"3", b"4", b"5"]
        assert r.lrange("a", 0, -1) == [b"1", b"2", b"3", b"4", b"5"]

    def test_lrem(self, r):
        r.rpush("a", "Z", "b", "Z", "Z", "c", "Z", "Z")
        # remove the first 'Z'  item
        assert r.lrem("a", 1, "Z") == 1
        assert r.lrange("a", 0, -1) == [b"b", b"Z", b"Z", b"c", b"Z", b"Z"]
        # remove the last 2 'Z' items
        assert r.lrem("a", -2, "Z") == 2
        assert r.lrange("a", 0, -1) == [b"b", b"Z", b"Z", b"c"]
        # remove all 'Z' items
        assert r.lrem("a", 0, "Z") == 2
        assert r.lrange("a", 0, -1) == [b"b", b"c"]

    def test_lset(self, r):
        r.rpush("a", "1", "2", "3")
        assert r.lrange("a", 0, -1) == [b"1", b"2", b"3"]
        assert r.lset("a", 1, "4")
        assert r.lrange("a", 0, 2) == [b"1", b"4", b"3"]

    def test_ltrim(self, r):
        r.rpush("a", "1", "2", "3")
        assert r.ltrim("a", 0, 1)
        assert r.lrange("a", 0, -1) == [b"1", b"2"]

    def test_rpop(self, r):
        r.rpush("a", "1", "2", "3")
        assert r.rpop("a") == b"3"
        assert r.rpop("a") == b"2"
        assert r.rpop("a") == b"1"
        assert r.rpop("a") is None

    @skip_if_server_version_lt("6.2.0")
    def test_rpop_count(self, r):
        r.rpush("a", "1", "2", "3")
        assert r.rpop("a", 2) == [b"3", b"2"]
        assert r.rpop("a", 1) == [b"1"]
        assert r.rpop("a") is None
        assert r.rpop("a", 3) is None

    @pytest.mark.onlynoncluster
    def test_rpoplpush(self, r):
        r.rpush("a", "a1", "a2", "a3")
        r.rpush("b", "b1", "b2", "b3")
        assert r.rpoplpush("a", "b") == b"a3"
        assert r.lrange("a", 0, -1) == [b"a1", b"a2"]
        assert r.lrange("b", 0, -1) == [b"a3", b"b1", b"b2", b"b3"]

    def test_rpush(self, r):
        assert r.rpush("a", "1") == 1
        assert r.rpush("a", "2") == 2
        assert r.rpush("a", "3", "4") == 4
        assert r.lrange("a", 0, -1) == [b"1", b"2", b"3", b"4"]

    @skip_if_server_version_lt("6.0.6")
    def test_lpos(self, r):
        assert r.rpush("a", "a", "b", "c", "1", "2", "3", "c", "c") == 8
        assert r.lpos("a", "a") == 0
        assert r.lpos("a", "c") == 2

        assert r.lpos("a", "c", rank=1) == 2
        assert r.lpos("a", "c", rank=2) == 6
        assert r.lpos("a", "c", rank=4) is None
        assert r.lpos("a", "c", rank=-1) == 7
        assert r.lpos("a", "c", rank=-2) == 6

        assert r.lpos("a", "c", count=0) == [2, 6, 7]
        assert r.lpos("a", "c", count=1) == [2]
        assert r.lpos("a", "c", count=2) == [2, 6]
        assert r.lpos("a", "c", count=100) == [2, 6, 7]

        assert r.lpos("a", "c", count=0, rank=2) == [6, 7]
        assert r.lpos("a", "c", count=2, rank=-1) == [7, 6]

        assert r.lpos("axxx", "c", count=0, rank=2) == []
        assert r.lpos("axxx", "c") is None

        assert r.lpos("a", "x", count=2) == []
        assert r.lpos("a", "x") is None

        assert r.lpos("a", "a", count=0, maxlen=1) == [0]
        assert r.lpos("a", "c", count=0, maxlen=1) == []
        assert r.lpos("a", "c", count=0, maxlen=3) == [2]
        assert r.lpos("a", "c", count=0, maxlen=3, rank=-1) == [7, 6]
        assert r.lpos("a", "c", count=0, maxlen=7, rank=2) == [6]

    def test_rpushx(self, r):
        assert r.rpushx("a", "b") == 0
        assert r.lrange("a", 0, -1) == []
        r.rpush("a", "1", "2", "3")
        assert r.rpushx("a", "4") == 4
        assert r.lrange("a", 0, -1) == [b"1", b"2", b"3", b"4"]

    # SCAN COMMANDS
    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.0")
    def test_scan(self, r):
        r.set("a", 1)
        r.set("b", 2)
        r.set("c", 3)
        cursor, keys = r.scan()
        assert cursor == 0
        assert set(keys) == {b"a", b"b", b"c"}
        _, keys = r.scan(match="a")
        assert set(keys) == {b"a"}

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.0.0")
    def test_scan_type(self, r):
        r.sadd("a-set", 1)
        r.hset("a-hash", "foo", 2)
        r.lpush("a-list", "aux", 3)
        _, keys = r.scan(match="a*", _type="SET")
        assert set(keys) == {b"a-set"}

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.0")
    def test_scan_iter(self, r):
        r.set("a", 1)
        r.set("b", 2)
        r.set("c", 3)
        keys = list(r.scan_iter())
        assert set(keys) == {b"a", b"b", b"c"}
        keys = list(r.scan_iter(match="a"))
        assert set(keys) == {b"a"}

    @skip_if_server_version_lt("2.8.0")
    def test_sscan(self, r):
        r.sadd("a", 1, 2, 3)
        cursor, members = r.sscan("a")
        assert cursor == 0
        assert set(members) == {b"1", b"2", b"3"}
        _, members = r.sscan("a", match=b"1")
        assert set(members) == {b"1"}

    @skip_if_server_version_lt("2.8.0")
    def test_sscan_iter(self, r):
        r.sadd("a", 1, 2, 3)
        members = list(r.sscan_iter("a"))
        assert set(members) == {b"1", b"2", b"3"}
        members = list(r.sscan_iter("a", match=b"1"))
        assert set(members) == {b"1"}

    @skip_if_server_version_lt("2.8.0")
    def test_hscan(self, r):
        r.hset("a", mapping={"a": 1, "b": 2, "c": 3})
        cursor, dic = r.hscan("a")
        assert cursor == 0
        assert dic == {b"a": b"1", b"b": b"2", b"c": b"3"}
        _, dic = r.hscan("a", match="a")
        assert dic == {b"a": b"1"}

    @skip_if_server_version_lt("2.8.0")
    def test_hscan_iter(self, r):
        r.hset("a", mapping={"a": 1, "b": 2, "c": 3})
        dic = dict(r.hscan_iter("a"))
        assert dic == {b"a": b"1", b"b": b"2", b"c": b"3"}
        dic = dict(r.hscan_iter("a", match="a"))
        assert dic == {b"a": b"1"}

    @skip_if_server_version_lt("2.8.0")
    def test_zscan(self, r):
        r.zadd("a", {"a": 1, "b": 2, "c": 3})
        cursor, pairs = r.zscan("a")
        assert cursor == 0
        assert set(pairs) == {(b"a", 1), (b"b", 2), (b"c", 3)}
        _, pairs = r.zscan("a", match="a")
        assert set(pairs) == {(b"a", 1)}

    @skip_if_server_version_lt("2.8.0")
    def test_zscan_iter(self, r):
        r.zadd("a", {"a": 1, "b": 2, "c": 3})
        pairs = list(r.zscan_iter("a"))
        assert set(pairs) == {(b"a", 1), (b"b", 2), (b"c", 3)}
        pairs = list(r.zscan_iter("a", match="a"))
        assert set(pairs) == {(b"a", 1)}

    # SET COMMANDS
    def test_sadd(self, r):
        members = {b"1", b"2", b"3"}
        r.sadd("a", *members)
        assert r.smembers("a") == members

    def test_scard(self, r):
        r.sadd("a", "1", "2", "3")
        assert r.scard("a") == 3

    @pytest.mark.onlynoncluster
    def test_sdiff(self, r):
        r.sadd("a", "1", "2", "3")
        assert r.sdiff("a", "b") == {b"1", b"2", b"3"}
        r.sadd("b", "2", "3")
        assert r.sdiff("a", "b") == {b"1"}

    @pytest.mark.onlynoncluster
    def test_sdiffstore(self, r):
        r.sadd("a", "1", "2", "3")
        assert r.sdiffstore("c", "a", "b") == 3
        assert r.smembers("c") == {b"1", b"2", b"3"}
        r.sadd("b", "2", "3")
        assert r.sdiffstore("c", "a", "b") == 1
        assert r.smembers("c") == {b"1"}

    @pytest.mark.onlynoncluster
    def test_sinter(self, r):
        r.sadd("a", "1", "2", "3")
        assert r.sinter("a", "b") == set()
        r.sadd("b", "2", "3")
        assert r.sinter("a", "b") == {b"2", b"3"}

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_sintercard(self, r):
        r.sadd("a", 1, 2, 3)
        r.sadd("b", 1, 2, 3)
        r.sadd("c", 1, 3, 4)
        assert r.sintercard(3, ["a", "b", "c"]) == 2
        assert r.sintercard(3, ["a", "b", "c"], limit=1) == 1

    @pytest.mark.onlynoncluster
    def test_sinterstore(self, r):
        r.sadd("a", "1", "2", "3")
        assert r.sinterstore("c", "a", "b") == 0
        assert r.smembers("c") == set()
        r.sadd("b", "2", "3")
        assert r.sinterstore("c", "a", "b") == 2
        assert r.smembers("c") == {b"2", b"3"}

    def test_sismember(self, r):
        r.sadd("a", "1", "2", "3")
        assert r.sismember("a", "1")
        assert r.sismember("a", "2")
        assert r.sismember("a", "3")
        assert not r.sismember("a", "4")

    def test_smembers(self, r):
        r.sadd("a", "1", "2", "3")
        assert r.smembers("a") == {b"1", b"2", b"3"}

    @skip_if_server_version_lt("6.2.0")
    def test_smismember(self, r):
        r.sadd("a", "1", "2", "3")
        result_list = [True, False, True, True]
        assert r.smismember("a", "1", "4", "2", "3") == result_list
        assert r.smismember("a", ["1", "4", "2", "3"]) == result_list

    @pytest.mark.onlynoncluster
    def test_smove(self, r):
        r.sadd("a", "a1", "a2")
        r.sadd("b", "b1", "b2")
        assert r.smove("a", "b", "a1")
        assert r.smembers("a") == {b"a2"}
        assert r.smembers("b") == {b"b1", b"b2", b"a1"}

    def test_spop(self, r):
        s = [b"1", b"2", b"3"]
        r.sadd("a", *s)
        value = r.spop("a")
        assert value in s
        assert r.smembers("a") == set(s) - {value}

    @skip_if_server_version_lt("3.2.0")
    def test_spop_multi_value(self, r):
        s = [b"1", b"2", b"3"]
        r.sadd("a", *s)
        values = r.spop("a", 2)
        assert len(values) == 2

        for value in values:
            assert value in s

        assert r.spop("a", 1) == list(set(s) - set(values))

    def test_srandmember(self, r):
        s = [b"1", b"2", b"3"]
        r.sadd("a", *s)
        assert r.srandmember("a") in s

    @skip_if_server_version_lt("2.6.0")
    def test_srandmember_multi_value(self, r):
        s = [b"1", b"2", b"3"]
        r.sadd("a", *s)
        randoms = r.srandmember("a", number=2)
        assert len(randoms) == 2
        assert set(randoms).intersection(s) == set(randoms)

    def test_srem(self, r):
        r.sadd("a", "1", "2", "3", "4")
        assert r.srem("a", "5") == 0
        assert r.srem("a", "2", "4") == 2
        assert r.smembers("a") == {b"1", b"3"}

    @pytest.mark.onlynoncluster
    def test_sunion(self, r):
        r.sadd("a", "1", "2")
        r.sadd("b", "2", "3")
        assert r.sunion("a", "b") == {b"1", b"2", b"3"}

    @pytest.mark.onlynoncluster
    def test_sunionstore(self, r):
        r.sadd("a", "1", "2")
        r.sadd("b", "2", "3")
        assert r.sunionstore("c", "a", "b") == 3
        assert r.smembers("c") == {b"1", b"2", b"3"}

    @skip_if_server_version_lt("1.0.0")
    @skip_if_redis_enterprise()
    def test_debug_segfault(self, r):
        with pytest.raises(NotImplementedError):
            r.debug_segfault()

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("3.2.0")
    @skip_if_redis_enterprise()
    def test_script_debug(self, r):
        with pytest.raises(NotImplementedError):
            r.script_debug()

    # SORTED SET COMMANDS
    def test_zadd(self, r):
        mapping = {"a1": 1.0, "a2": 2.0, "a3": 3.0}
        r.zadd("a", mapping)
        assert r.zrange("a", 0, -1, withscores=True) == [
            (b"a1", 1.0),
            (b"a2", 2.0),
            (b"a3", 3.0),
        ]

        # error cases
        with pytest.raises(exceptions.DataError):
            r.zadd("a", {})

        # cannot use both nx and xx options
        with pytest.raises(exceptions.DataError):
            r.zadd("a", mapping, nx=True, xx=True)

        # cannot use the incr options with more than one value
        with pytest.raises(exceptions.DataError):
            r.zadd("a", mapping, incr=True)

    def test_zadd_nx(self, r):
        assert r.zadd("a", {"a1": 1}) == 1
        assert r.zadd("a", {"a1": 99, "a2": 2}, nx=True) == 1
        assert r.zrange("a", 0, -1, withscores=True) == [(b"a1", 1.0), (b"a2", 2.0)]

    def test_zadd_xx(self, r):
        assert r.zadd("a", {"a1": 1}) == 1
        assert r.zadd("a", {"a1": 99, "a2": 2}, xx=True) == 0
        assert r.zrange("a", 0, -1, withscores=True) == [(b"a1", 99.0)]

    def test_zadd_ch(self, r):
        assert r.zadd("a", {"a1": 1}) == 1
        assert r.zadd("a", {"a1": 99, "a2": 2}, ch=True) == 2
        assert r.zrange("a", 0, -1, withscores=True) == [(b"a2", 2.0), (b"a1", 99.0)]

    def test_zadd_incr(self, r):
        assert r.zadd("a", {"a1": 1}) == 1
        assert r.zadd("a", {"a1": 4.5}, incr=True) == 5.5

    def test_zadd_incr_with_xx(self, r):
        # this asks zadd to incr 'a1' only if it exists, but it clearly
        # doesn't. Redis returns a null value in this case and so should
        # redis-py
        assert r.zadd("a", {"a1": 1}, xx=True, incr=True) is None

    @skip_if_server_version_lt("6.2.0")
    def test_zadd_gt_lt(self, r):
        r.zadd("a", {"a": 2})
        assert r.zadd("a", {"a": 5}, gt=True, ch=True) == 1
        assert r.zadd("a", {"a": 1}, gt=True, ch=True) == 0
        assert r.zadd("a", {"a": 5}, lt=True, ch=True) == 0
        assert r.zadd("a", {"a": 1}, lt=True, ch=True) == 1

        # cannot combine both nx and xx options and gt and lt options
        with pytest.raises(exceptions.DataError):
            r.zadd("a", {"a15": 15}, nx=True, lt=True)
        with pytest.raises(exceptions.DataError):
            r.zadd("a", {"a15": 15}, nx=True, gt=True)
        with pytest.raises(exceptions.DataError):
            r.zadd("a", {"a15": 15}, lt=True, gt=True)
        with pytest.raises(exceptions.DataError):
            r.zadd("a", {"a15": 15}, nx=True, xx=True)

    def test_zcard(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert r.zcard("a") == 3

    def test_zcount(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert r.zcount("a", "-inf", "+inf") == 3
        assert r.zcount("a", 1, 2) == 2
        assert r.zcount("a", "(" + str(1), 2) == 1
        assert r.zcount("a", 1, "(" + str(2)) == 1
        assert r.zcount("a", 10, 20) == 0

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    def test_zdiff(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        r.zadd("b", {"a1": 1, "a2": 2})
        assert r.zdiff(["a", "b"]) == [b"a3"]
        assert r.zdiff(["a", "b"], withscores=True) == [b"a3", b"3"]

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    def test_zdiffstore(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        r.zadd("b", {"a1": 1, "a2": 2})
        assert r.zdiffstore("out", ["a", "b"])
        assert r.zrange("out", 0, -1) == [b"a3"]
        assert r.zrange("out", 0, -1, withscores=True) == [(b"a3", 3.0)]

    def test_zincrby(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert r.zincrby("a", 1, "a2") == 3.0
        assert r.zincrby("a", 5, "a3") == 8.0
        assert r.zscore("a", "a2") == 3.0
        assert r.zscore("a", "a3") == 8.0

    @skip_if_server_version_lt("2.8.9")
    def test_zlexcount(self, r):
        r.zadd("a", {"a": 0, "b": 0, "c": 0, "d": 0, "e": 0, "f": 0, "g": 0})
        assert r.zlexcount("a", "-", "+") == 7
        assert r.zlexcount("a", "[b", "[f") == 5

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    def test_zinter(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 1})
        r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zinter(["a", "b", "c"]) == [b"a3", b"a1"]
        # invalid aggregation
        with pytest.raises(exceptions.DataError):
            r.zinter(["a", "b", "c"], aggregate="foo", withscores=True)
        # aggregate with SUM
        assert r.zinter(["a", "b", "c"], withscores=True) == [(b"a3", 8), (b"a1", 9)]
        # aggregate with MAX
        assert r.zinter(["a", "b", "c"], aggregate="MAX", withscores=True) == [
            (b"a3", 5),
            (b"a1", 6),
        ]
        # aggregate with MIN
        assert r.zinter(["a", "b", "c"], aggregate="MIN", withscores=True) == [
            (b"a1", 1),
            (b"a3", 1),
        ]
        # with weights
        assert r.zinter({"a": 1, "b": 2, "c": 3}, withscores=True) == [
            (b"a3", 20),
            (b"a1", 23),
        ]

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_zintercard(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 1})
        r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zintercard(3, ["a", "b", "c"]) == 2
        assert r.zintercard(3, ["a", "b", "c"], limit=1) == 1

    @pytest.mark.onlynoncluster
    def test_zinterstore_sum(self, r):
        r.zadd("a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zinterstore("d", ["a", "b", "c"]) == 2
        assert r.zrange("d", 0, -1, withscores=True) == [(b"a3", 8), (b"a1", 9)]

    @pytest.mark.onlynoncluster
    def test_zinterstore_max(self, r):
        r.zadd("a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zinterstore("d", ["a", "b", "c"], aggregate="MAX") == 2
        assert r.zrange("d", 0, -1, withscores=True) == [(b"a3", 5), (b"a1", 6)]

    @pytest.mark.onlynoncluster
    def test_zinterstore_min(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        r.zadd("b", {"a1": 2, "a2": 3, "a3": 5})
        r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zinterstore("d", ["a", "b", "c"], aggregate="MIN") == 2
        assert r.zrange("d", 0, -1, withscores=True) == [(b"a1", 1), (b"a3", 3)]

    @pytest.mark.onlynoncluster
    def test_zinterstore_with_weight(self, r):
        r.zadd("a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zinterstore("d", {"a": 1, "b": 2, "c": 3}) == 2
        assert r.zrange("d", 0, -1, withscores=True) == [(b"a3", 20), (b"a1", 23)]

    @skip_if_server_version_lt("4.9.0")
    def test_zpopmax(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert r.zpopmax("a") == [(b"a3", 3)]

        # with count
        assert r.zpopmax("a", count=2) == [(b"a2", 2), (b"a1", 1)]

    @skip_if_server_version_lt("4.9.0")
    def test_zpopmin(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert r.zpopmin("a") == [(b"a1", 1)]

        # with count
        assert r.zpopmin("a", count=2) == [(b"a2", 2), (b"a3", 3)]

    @skip_if_server_version_lt("6.2.0")
    def test_zrandemember(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3, "a4": 4, "a5": 5})
        assert r.zrandmember("a") is not None
        assert len(r.zrandmember("a", 2)) == 2
        # with scores
        assert len(r.zrandmember("a", 2, True)) == 4
        # without duplications
        assert len(r.zrandmember("a", 10)) == 5
        # with duplications
        assert len(r.zrandmember("a", -10)) == 10

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("4.9.0")
    def test_bzpopmax(self, r):
        r.zadd("a", {"a1": 1, "a2": 2})
        r.zadd("b", {"b1": 10, "b2": 20})
        assert r.bzpopmax(["b", "a"], timeout=1) == (b"b", b"b2", 20)
        assert r.bzpopmax(["b", "a"], timeout=1) == (b"b", b"b1", 10)
        assert r.bzpopmax(["b", "a"], timeout=1) == (b"a", b"a2", 2)
        assert r.bzpopmax(["b", "a"], timeout=1) == (b"a", b"a1", 1)
        assert r.bzpopmax(["b", "a"], timeout=1) is None
        r.zadd("c", {"c1": 100})
        assert r.bzpopmax("c", timeout=1) == (b"c", b"c1", 100)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("4.9.0")
    def test_bzpopmin(self, r):
        r.zadd("a", {"a1": 1, "a2": 2})
        r.zadd("b", {"b1": 10, "b2": 20})
        assert r.bzpopmin(["b", "a"], timeout=1) == (b"b", b"b1", 10)
        assert r.bzpopmin(["b", "a"], timeout=1) == (b"b", b"b2", 20)
        assert r.bzpopmin(["b", "a"], timeout=1) == (b"a", b"a1", 1)
        assert r.bzpopmin(["b", "a"], timeout=1) == (b"a", b"a2", 2)
        assert r.bzpopmin(["b", "a"], timeout=1) is None
        r.zadd("c", {"c1": 100})
        assert r.bzpopmin("c", timeout=1) == (b"c", b"c1", 100)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_zmpop(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        res = [b"a", [[b"a1", b"1"], [b"a2", b"2"]]]
        assert r.zmpop("2", ["b", "a"], min=True, count=2) == res
        with pytest.raises(redis.DataError):
            r.zmpop("2", ["b", "a"], count=2)
        r.zadd("b", {"b1": 10, "ab": 9, "b3": 8})
        assert r.zmpop("2", ["b", "a"], max=True) == [b"b", [[b"b1", b"10"]]]

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_bzmpop(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        res = [b"a", [[b"a1", b"1"], [b"a2", b"2"]]]
        assert r.bzmpop(1, "2", ["b", "a"], min=True, count=2) == res
        with pytest.raises(redis.DataError):
            r.bzmpop(1, "2", ["b", "a"], count=2)
        r.zadd("b", {"b1": 10, "ab": 9, "b3": 8})
        res = [b"b", [[b"b1", b"10"]]]
        assert r.bzmpop(0, "2", ["b", "a"], max=True) == res
        assert r.bzmpop(1, "2", ["foo", "bar"], max=True) is None

    def test_zrange(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert r.zrange("a", 0, 1) == [b"a1", b"a2"]
        assert r.zrange("a", 1, 2) == [b"a2", b"a3"]
        assert r.zrange("a", 0, 2) == [b"a1", b"a2", b"a3"]
        assert r.zrange("a", 0, 2, desc=True) == [b"a3", b"a2", b"a1"]

        # withscores
        assert r.zrange("a", 0, 1, withscores=True) == [(b"a1", 1.0), (b"a2", 2.0)]
        assert r.zrange("a", 1, 2, withscores=True) == [(b"a2", 2.0), (b"a3", 3.0)]

        # custom score function
        assert r.zrange("a", 0, 1, withscores=True, score_cast_func=int) == [
            (b"a1", 1),
            (b"a2", 2),
        ]

    def test_zrange_errors(self, r):
        with pytest.raises(exceptions.DataError):
            r.zrange("a", 0, 1, byscore=True, bylex=True)
        with pytest.raises(exceptions.DataError):
            r.zrange("a", 0, 1, bylex=True, withscores=True)
        with pytest.raises(exceptions.DataError):
            r.zrange("a", 0, 1, byscore=True, withscores=True, offset=4)
        with pytest.raises(exceptions.DataError):
            r.zrange("a", 0, 1, byscore=True, withscores=True, num=2)

    @skip_if_server_version_lt("6.2.0")
    def test_zrange_params(self, r):
        # bylex
        r.zadd("a", {"a": 0, "b": 0, "c": 0, "d": 0, "e": 0, "f": 0, "g": 0})
        assert r.zrange("a", "[aaa", "(g", bylex=True) == [b"b", b"c", b"d", b"e", b"f"]
        assert r.zrange("a", "[f", "+", bylex=True) == [b"f", b"g"]
        assert r.zrange("a", "+", "[f", desc=True, bylex=True) == [b"g", b"f"]
        assert r.zrange("a", "-", "+", bylex=True, offset=3, num=2) == [b"d", b"e"]
        assert r.zrange("a", "+", "-", desc=True, bylex=True, offset=3, num=2) == [
            b"d",
            b"c",
        ]

        # byscore
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3, "a4": 4, "a5": 5})
        assert r.zrange("a", 2, 4, byscore=True, offset=1, num=2) == [b"a3", b"a4"]
        assert r.zrange("a", 4, 2, desc=True, byscore=True, offset=1, num=2) == [
            b"a3",
            b"a2",
        ]
        assert r.zrange("a", 2, 4, byscore=True, withscores=True) == [
            (b"a2", 2.0),
            (b"a3", 3.0),
            (b"a4", 4.0),
        ]
        assert r.zrange(
            "a", 4, 2, desc=True, byscore=True, withscores=True, score_cast_func=int
        ) == [(b"a4", 4), (b"a3", 3), (b"a2", 2)]

        # rev
        assert r.zrange("a", 0, 1, desc=True) == [b"a5", b"a4"]

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    def test_zrangestore(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert r.zrangestore("b", "a", 0, 1)
        assert r.zrange("b", 0, -1) == [b"a1", b"a2"]
        assert r.zrangestore("b", "a", 1, 2)
        assert r.zrange("b", 0, -1) == [b"a2", b"a3"]
        assert r.zrange("b", 0, -1, withscores=True) == [(b"a2", 2), (b"a3", 3)]
        # reversed order
        assert r.zrangestore("b", "a", 1, 2, desc=True)
        assert r.zrange("b", 0, -1) == [b"a1", b"a2"]
        # by score
        assert r.zrangestore("b", "a", 2, 1, byscore=True, offset=0, num=1, desc=True)
        assert r.zrange("b", 0, -1) == [b"a2"]
        # by lex
        assert r.zrangestore("b", "a", "[a2", "(a3", bylex=True, offset=0, num=1)
        assert r.zrange("b", 0, -1) == [b"a2"]

    @skip_if_server_version_lt("2.8.9")
    def test_zrangebylex(self, r):
        r.zadd("a", {"a": 0, "b": 0, "c": 0, "d": 0, "e": 0, "f": 0, "g": 0})
        assert r.zrangebylex("a", "-", "[c") == [b"a", b"b", b"c"]
        assert r.zrangebylex("a", "-", "(c") == [b"a", b"b"]
        assert r.zrangebylex("a", "[aaa", "(g") == [b"b", b"c", b"d", b"e", b"f"]
        assert r.zrangebylex("a", "[f", "+") == [b"f", b"g"]
        assert r.zrangebylex("a", "-", "+", start=3, num=2) == [b"d", b"e"]

    @skip_if_server_version_lt("2.9.9")
    def test_zrevrangebylex(self, r):
        r.zadd("a", {"a": 0, "b": 0, "c": 0, "d": 0, "e": 0, "f": 0, "g": 0})
        assert r.zrevrangebylex("a", "[c", "-") == [b"c", b"b", b"a"]
        assert r.zrevrangebylex("a", "(c", "-") == [b"b", b"a"]
        assert r.zrevrangebylex("a", "(g", "[aaa") == [b"f", b"e", b"d", b"c", b"b"]
        assert r.zrevrangebylex("a", "+", "[f") == [b"g", b"f"]
        assert r.zrevrangebylex("a", "+", "-", start=3, num=2) == [b"d", b"c"]

    def test_zrangebyscore(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3, "a4": 4, "a5": 5})
        assert r.zrangebyscore("a", 2, 4) == [b"a2", b"a3", b"a4"]
        # slicing with start/num
        assert r.zrangebyscore("a", 2, 4, start=1, num=2) == [b"a3", b"a4"]
        # withscores
        assert r.zrangebyscore("a", 2, 4, withscores=True) == [
            (b"a2", 2.0),
            (b"a3", 3.0),
            (b"a4", 4.0),
        ]
        assert r.zrangebyscore("a", 2, 4, withscores=True, score_cast_func=int) == [
            (b"a2", 2),
            (b"a3", 3),
            (b"a4", 4),
        ]

    def test_zrank(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3, "a4": 4, "a5": 5})
        assert r.zrank("a", "a1") == 0
        assert r.zrank("a", "a2") == 1
        assert r.zrank("a", "a6") is None

    def test_zrem(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert r.zrem("a", "a2") == 1
        assert r.zrange("a", 0, -1) == [b"a1", b"a3"]
        assert r.zrem("a", "b") == 0
        assert r.zrange("a", 0, -1) == [b"a1", b"a3"]

    def test_zrem_multiple_keys(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert r.zrem("a", "a1", "a2") == 2
        assert r.zrange("a", 0, 5) == [b"a3"]

    @skip_if_server_version_lt("2.8.9")
    def test_zremrangebylex(self, r):
        r.zadd("a", {"a": 0, "b": 0, "c": 0, "d": 0, "e": 0, "f": 0, "g": 0})
        assert r.zremrangebylex("a", "-", "[c") == 3
        assert r.zrange("a", 0, -1) == [b"d", b"e", b"f", b"g"]
        assert r.zremrangebylex("a", "[f", "+") == 2
        assert r.zrange("a", 0, -1) == [b"d", b"e"]
        assert r.zremrangebylex("a", "[h", "+") == 0
        assert r.zrange("a", 0, -1) == [b"d", b"e"]

    def test_zremrangebyrank(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3, "a4": 4, "a5": 5})
        assert r.zremrangebyrank("a", 1, 3) == 3
        assert r.zrange("a", 0, 5) == [b"a1", b"a5"]

    def test_zremrangebyscore(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3, "a4": 4, "a5": 5})
        assert r.zremrangebyscore("a", 2, 4) == 3
        assert r.zrange("a", 0, -1) == [b"a1", b"a5"]
        assert r.zremrangebyscore("a", 2, 4) == 0
        assert r.zrange("a", 0, -1) == [b"a1", b"a5"]

    def test_zrevrange(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert r.zrevrange("a", 0, 1) == [b"a3", b"a2"]
        assert r.zrevrange("a", 1, 2) == [b"a2", b"a1"]

        # withscores
        assert r.zrevrange("a", 0, 1, withscores=True) == [(b"a3", 3.0), (b"a2", 2.0)]
        assert r.zrevrange("a", 1, 2, withscores=True) == [(b"a2", 2.0), (b"a1", 1.0)]

        # custom score function
        assert r.zrevrange("a", 0, 1, withscores=True, score_cast_func=int) == [
            (b"a3", 3.0),
            (b"a2", 2.0),
        ]

    def test_zrevrangebyscore(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3, "a4": 4, "a5": 5})
        assert r.zrevrangebyscore("a", 4, 2) == [b"a4", b"a3", b"a2"]
        # slicing with start/num
        assert r.zrevrangebyscore("a", 4, 2, start=1, num=2) == [b"a3", b"a2"]
        # withscores
        assert r.zrevrangebyscore("a", 4, 2, withscores=True) == [
            (b"a4", 4.0),
            (b"a3", 3.0),
            (b"a2", 2.0),
        ]
        # custom score function
        assert r.zrevrangebyscore("a", 4, 2, withscores=True, score_cast_func=int) == [
            (b"a4", 4),
            (b"a3", 3),
            (b"a2", 2),
        ]

    def test_zrevrank(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3, "a4": 4, "a5": 5})
        assert r.zrevrank("a", "a1") == 4
        assert r.zrevrank("a", "a2") == 3
        assert r.zrevrank("a", "a6") is None

    def test_zscore(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert r.zscore("a", "a1") == 1.0
        assert r.zscore("a", "a2") == 2.0
        assert r.zscore("a", "a4") is None

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    def test_zunion(self, r):
        r.zadd("a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        # sum
        assert r.zunion(["a", "b", "c"]) == [b"a2", b"a4", b"a3", b"a1"]
        assert r.zunion(["a", "b", "c"], withscores=True) == [
            (b"a2", 3),
            (b"a4", 4),
            (b"a3", 8),
            (b"a1", 9),
        ]
        # max
        assert r.zunion(["a", "b", "c"], aggregate="MAX", withscores=True) == [
            (b"a2", 2),
            (b"a4", 4),
            (b"a3", 5),
            (b"a1", 6),
        ]
        # min
        assert r.zunion(["a", "b", "c"], aggregate="MIN", withscores=True) == [
            (b"a1", 1),
            (b"a2", 1),
            (b"a3", 1),
            (b"a4", 4),
        ]
        # with weight
        assert r.zunion({"a": 1, "b": 2, "c": 3}, withscores=True) == [
            (b"a2", 5),
            (b"a4", 12),
            (b"a3", 20),
            (b"a1", 23),
        ]

    @pytest.mark.onlynoncluster
    def test_zunionstore_sum(self, r):
        r.zadd("a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zunionstore("d", ["a", "b", "c"]) == 4
        assert r.zrange("d", 0, -1, withscores=True) == [
            (b"a2", 3),
            (b"a4", 4),
            (b"a3", 8),
            (b"a1", 9),
        ]

    @pytest.mark.onlynoncluster
    def test_zunionstore_max(self, r):
        r.zadd("a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zunionstore("d", ["a", "b", "c"], aggregate="MAX") == 4
        assert r.zrange("d", 0, -1, withscores=True) == [
            (b"a2", 2),
            (b"a4", 4),
            (b"a3", 5),
            (b"a1", 6),
        ]

    @pytest.mark.onlynoncluster
    def test_zunionstore_min(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        r.zadd("b", {"a1": 2, "a2": 2, "a3": 4})
        r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zunionstore("d", ["a", "b", "c"], aggregate="MIN") == 4
        assert r.zrange("d", 0, -1, withscores=True) == [
            (b"a1", 1),
            (b"a2", 2),
            (b"a3", 3),
            (b"a4", 4),
        ]

    @pytest.mark.onlynoncluster
    def test_zunionstore_with_weight(self, r):
        r.zadd("a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zunionstore("d", {"a": 1, "b": 2, "c": 3}) == 4
        assert r.zrange("d", 0, -1, withscores=True) == [
            (b"a2", 5),
            (b"a4", 12),
            (b"a3", 20),
            (b"a1", 23),
        ]

    @skip_if_server_version_lt("6.1.240")
    def test_zmscore(self, r):
        with pytest.raises(exceptions.DataError):
            r.zmscore("invalid_key", [])

        assert r.zmscore("invalid_key", ["invalid_member"]) == [None]

        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3.5})
        assert r.zmscore("a", ["a1", "a2", "a3", "a4"]) == [1.0, 2.0, 3.5, None]

    # HYPERLOGLOG TESTS
    @skip_if_server_version_lt("2.8.9")
    def test_pfadd(self, r):
        members = {b"1", b"2", b"3"}
        assert r.pfadd("a", *members) == 1
        assert r.pfadd("a", *members) == 0
        assert r.pfcount("a") == len(members)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.9")
    def test_pfcount(self, r):
        members = {b"1", b"2", b"3"}
        r.pfadd("a", *members)
        assert r.pfcount("a") == len(members)
        members_b = {b"2", b"3", b"4"}
        r.pfadd("b", *members_b)
        assert r.pfcount("b") == len(members_b)
        assert r.pfcount("a", "b") == len(members_b.union(members))

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.9")
    def test_pfmerge(self, r):
        mema = {b"1", b"2", b"3"}
        memb = {b"2", b"3", b"4"}
        memc = {b"5", b"6", b"7"}
        r.pfadd("a", *mema)
        r.pfadd("b", *memb)
        r.pfadd("c", *memc)
        r.pfmerge("d", "c", "a")
        assert r.pfcount("d") == 6
        r.pfmerge("d", "b")
        assert r.pfcount("d") == 7

    # HASH COMMANDS
    def test_hget_and_hset(self, r):
        r.hset("a", mapping={"1": 1, "2": 2, "3": 3})
        assert r.hget("a", "1") == b"1"
        assert r.hget("a", "2") == b"2"
        assert r.hget("a", "3") == b"3"

        # field was updated, redis returns 0
        assert r.hset("a", "2", 5) == 0
        assert r.hget("a", "2") == b"5"

        # field is new, redis returns 1
        assert r.hset("a", "4", 4) == 1
        assert r.hget("a", "4") == b"4"

        # key inside of hash that doesn't exist returns null value
        assert r.hget("a", "b") is None

        # keys with bool(key) == False
        assert r.hset("a", 0, 10) == 1
        assert r.hset("a", "", 10) == 1

    def test_hset_with_multi_key_values(self, r):
        r.hset("a", mapping={"1": 1, "2": 2, "3": 3})
        assert r.hget("a", "1") == b"1"
        assert r.hget("a", "2") == b"2"
        assert r.hget("a", "3") == b"3"

        r.hset("b", "foo", "bar", mapping={"1": 1, "2": 2})
        assert r.hget("b", "1") == b"1"
        assert r.hget("b", "2") == b"2"
        assert r.hget("b", "foo") == b"bar"

    def test_hset_with_key_values_passed_as_list(self, r):
        r.hset("a", items=["1", 1, "2", 2, "3", 3])
        assert r.hget("a", "1") == b"1"
        assert r.hget("a", "2") == b"2"
        assert r.hget("a", "3") == b"3"

        r.hset("b", "foo", "bar", items=["1", 1, "2", 2])
        assert r.hget("b", "1") == b"1"
        assert r.hget("b", "2") == b"2"
        assert r.hget("b", "foo") == b"bar"

    def test_hset_without_data(self, r):
        with pytest.raises(exceptions.DataError):
            r.hset("x")

    def test_hdel(self, r):
        r.hset("a", mapping={"1": 1, "2": 2, "3": 3})
        assert r.hdel("a", "2") == 1
        assert r.hget("a", "2") is None
        assert r.hdel("a", "1", "3") == 2
        assert r.hlen("a") == 0

    def test_hexists(self, r):
        r.hset("a", mapping={"1": 1, "2": 2, "3": 3})
        assert r.hexists("a", "1")
        assert not r.hexists("a", "4")

    def test_hgetall(self, r):
        h = {b"a1": b"1", b"a2": b"2", b"a3": b"3"}
        r.hset("a", mapping=h)
        assert r.hgetall("a") == h

    def test_hincrby(self, r):
        assert r.hincrby("a", "1") == 1
        assert r.hincrby("a", "1", amount=2) == 3
        assert r.hincrby("a", "1", amount=-2) == 1

    @skip_if_server_version_lt("2.6.0")
    def test_hincrbyfloat(self, r):
        assert r.hincrbyfloat("a", "1") == 1.0
        assert r.hincrbyfloat("a", "1") == 2.0
        assert r.hincrbyfloat("a", "1", 1.2) == 3.2

    def test_hkeys(self, r):
        h = {b"a1": b"1", b"a2": b"2", b"a3": b"3"}
        r.hset("a", mapping=h)
        local_keys = list(h.keys())
        remote_keys = r.hkeys("a")
        assert sorted(local_keys) == sorted(remote_keys)

    def test_hlen(self, r):
        r.hset("a", mapping={"1": 1, "2": 2, "3": 3})
        assert r.hlen("a") == 3

    def test_hmget(self, r):
        assert r.hset("a", mapping={"a": 1, "b": 2, "c": 3})
        assert r.hmget("a", "a", "b", "c") == [b"1", b"2", b"3"]

    def test_hmset(self, r):
        redis_class = type(r).__name__
        warning_message = (
            r"^{0}\.hmset\(\) is deprecated\. "
            r"Use {0}\.hset\(\) instead\.$".format(redis_class)
        )
        h = {b"a": b"1", b"b": b"2", b"c": b"3"}
        with pytest.warns(DeprecationWarning, match=warning_message):
            assert r.hmset("a", h)
        assert r.hgetall("a") == h

    def test_hsetnx(self, r):
        # Initially set the hash field
        assert r.hsetnx("a", "1", 1)
        assert r.hget("a", "1") == b"1"
        assert not r.hsetnx("a", "1", 2)
        assert r.hget("a", "1") == b"1"

    def test_hvals(self, r):
        h = {b"a1": b"1", b"a2": b"2", b"a3": b"3"}
        r.hset("a", mapping=h)
        local_vals = list(h.values())
        remote_vals = r.hvals("a")
        assert sorted(local_vals) == sorted(remote_vals)

    @skip_if_server_version_lt("3.2.0")
    def test_hstrlen(self, r):
        r.hset("a", mapping={"1": "22", "2": "333"})
        assert r.hstrlen("a", "1") == 2
        assert r.hstrlen("a", "2") == 3

    # SORT
    def test_sort_basic(self, r):
        r.rpush("a", "3", "2", "1", "4")
        assert r.sort("a") == [b"1", b"2", b"3", b"4"]

    def test_sort_limited(self, r):
        r.rpush("a", "3", "2", "1", "4")
        assert r.sort("a", start=1, num=2) == [b"2", b"3"]

    @pytest.mark.onlynoncluster
    def test_sort_by(self, r):
        r["score:1"] = 8
        r["score:2"] = 3
        r["score:3"] = 5
        r.rpush("a", "3", "2", "1")
        assert r.sort("a", by="score:*") == [b"2", b"3", b"1"]

    @pytest.mark.onlynoncluster
    def test_sort_get(self, r):
        r["user:1"] = "u1"
        r["user:2"] = "u2"
        r["user:3"] = "u3"
        r.rpush("a", "2", "3", "1")
        assert r.sort("a", get="user:*") == [b"u1", b"u2", b"u3"]

    @pytest.mark.onlynoncluster
    def test_sort_get_multi(self, r):
        r["user:1"] = "u1"
        r["user:2"] = "u2"
        r["user:3"] = "u3"
        r.rpush("a", "2", "3", "1")
        assert r.sort("a", get=("user:*", "#")) == [
            b"u1",
            b"1",
            b"u2",
            b"2",
            b"u3",
            b"3",
        ]

    @pytest.mark.onlynoncluster
    def test_sort_get_groups_two(self, r):
        r["user:1"] = "u1"
        r["user:2"] = "u2"
        r["user:3"] = "u3"
        r.rpush("a", "2", "3", "1")
        assert r.sort("a", get=("user:*", "#"), groups=True) == [
            (b"u1", b"1"),
            (b"u2", b"2"),
            (b"u3", b"3"),
        ]

    @pytest.mark.onlynoncluster
    def test_sort_groups_string_get(self, r):
        r["user:1"] = "u1"
        r["user:2"] = "u2"
        r["user:3"] = "u3"
        r.rpush("a", "2", "3", "1")
        with pytest.raises(exceptions.DataError):
            r.sort("a", get="user:*", groups=True)

    @pytest.mark.onlynoncluster
    def test_sort_groups_just_one_get(self, r):
        r["user:1"] = "u1"
        r["user:2"] = "u2"
        r["user:3"] = "u3"
        r.rpush("a", "2", "3", "1")
        with pytest.raises(exceptions.DataError):
            r.sort("a", get=["user:*"], groups=True)

    def test_sort_groups_no_get(self, r):
        r["user:1"] = "u1"
        r["user:2"] = "u2"
        r["user:3"] = "u3"
        r.rpush("a", "2", "3", "1")
        with pytest.raises(exceptions.DataError):
            r.sort("a", groups=True)

    @pytest.mark.onlynoncluster
    def test_sort_groups_three_gets(self, r):
        r["user:1"] = "u1"
        r["user:2"] = "u2"
        r["user:3"] = "u3"
        r["door:1"] = "d1"
        r["door:2"] = "d2"
        r["door:3"] = "d3"
        r.rpush("a", "2", "3", "1")
        assert r.sort("a", get=("user:*", "door:*", "#"), groups=True) == [
            (b"u1", b"d1", b"1"),
            (b"u2", b"d2", b"2"),
            (b"u3", b"d3", b"3"),
        ]

    def test_sort_desc(self, r):
        r.rpush("a", "2", "3", "1")
        assert r.sort("a", desc=True) == [b"3", b"2", b"1"]

    def test_sort_alpha(self, r):
        r.rpush("a", "e", "c", "b", "d", "a")
        assert r.sort("a", alpha=True) == [b"a", b"b", b"c", b"d", b"e"]

    @pytest.mark.onlynoncluster
    def test_sort_store(self, r):
        r.rpush("a", "2", "3", "1")
        assert r.sort("a", store="sorted_values") == 3
        assert r.lrange("sorted_values", 0, -1) == [b"1", b"2", b"3"]

    @pytest.mark.onlynoncluster
    def test_sort_all_options(self, r):
        r["user:1:username"] = "zeus"
        r["user:2:username"] = "titan"
        r["user:3:username"] = "hermes"
        r["user:4:username"] = "hercules"
        r["user:5:username"] = "apollo"
        r["user:6:username"] = "athena"
        r["user:7:username"] = "hades"
        r["user:8:username"] = "dionysus"

        r["user:1:favorite_drink"] = "yuengling"
        r["user:2:favorite_drink"] = "rum"
        r["user:3:favorite_drink"] = "vodka"
        r["user:4:favorite_drink"] = "milk"
        r["user:5:favorite_drink"] = "pinot noir"
        r["user:6:favorite_drink"] = "water"
        r["user:7:favorite_drink"] = "gin"
        r["user:8:favorite_drink"] = "apple juice"

        r.rpush("gods", "5", "8", "3", "1", "2", "7", "6", "4")
        num = r.sort(
            "gods",
            start=2,
            num=4,
            by="user:*:username",
            get="user:*:favorite_drink",
            desc=True,
            alpha=True,
            store="sorted",
        )
        assert num == 4
        assert r.lrange("sorted", 0, 10) == [b"vodka", b"milk", b"gin", b"apple juice"]

    @skip_if_server_version_lt("7.0.0")
    @pytest.mark.onlynoncluster
    def test_sort_ro(self, r):
        r["score:1"] = 8
        r["score:2"] = 3
        r["score:3"] = 5
        r.rpush("a", "3", "2", "1")
        assert r.sort_ro("a", by="score:*") == [b"2", b"3", b"1"]
        r.rpush("b", "2", "3", "1")
        assert r.sort_ro("b", desc=True) == [b"3", b"2", b"1"]

    def test_sort_issue_924(self, r):
        # Tests for issue https://github.com/andymccurdy/redis-py/issues/924
        r.execute_command("SADD", "issue#924", 1)
        r.execute_command("SORT", "issue#924")

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_cluster_addslots(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster("ADDSLOTS", 1) is True

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_cluster_count_failure_reports(self, mock_cluster_resp_int):
        assert isinstance(
            mock_cluster_resp_int.cluster("COUNT-FAILURE-REPORTS", "node"), int
        )

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_cluster_countkeysinslot(self, mock_cluster_resp_int):
        assert isinstance(mock_cluster_resp_int.cluster("COUNTKEYSINSLOT", 2), int)

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_cluster_delslots(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster("DELSLOTS", 1) is True

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_cluster_failover(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster("FAILOVER", 1) is True

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_cluster_forget(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster("FORGET", 1) is True

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_cluster_info(self, mock_cluster_resp_info):
        assert isinstance(mock_cluster_resp_info.cluster("info"), dict)

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_cluster_keyslot(self, mock_cluster_resp_int):
        assert isinstance(mock_cluster_resp_int.cluster("keyslot", "asdf"), int)

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_cluster_meet(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster("meet", "ip", "port", 1) is True

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_cluster_nodes(self, mock_cluster_resp_nodes):
        assert isinstance(mock_cluster_resp_nodes.cluster("nodes"), dict)

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_cluster_replicate(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster("replicate", "nodeid") is True

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_cluster_reset(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster("reset", "hard") is True

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_cluster_saveconfig(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster("saveconfig") is True

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_cluster_setslot(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster("setslot", 1, "IMPORTING", "nodeid") is True

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_cluster_slaves(self, mock_cluster_resp_slaves):
        assert isinstance(mock_cluster_resp_slaves.cluster("slaves", "nodeid"), dict)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("3.0.0")
    @skip_if_server_version_gte("7.0.0")
    @skip_if_redis_enterprise()
    def test_readwrite(self, r):
        assert r.readwrite()

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("3.0.0")
    def test_readonly_invalid_cluster_state(self, r):
        with pytest.raises(exceptions.RedisError):
            r.readonly()

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("3.0.0")
    @skip_if_redis_enterprise()
    def test_readonly(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.readonly() is True

    # GEO COMMANDS
    @skip_if_server_version_lt("3.2.0")
    def test_geoadd(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )
        assert r.geoadd("barcelona", values) == 2
        assert r.zcard("barcelona") == 2

    @skip_if_server_version_lt("6.2.0")
    def test_geoadd_nx(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )
        assert r.geoadd("a", values) == 2
        values = (
            (2.1909389952632, 41.433791470673, "place1")
            + (2.1873744593677, 41.406342043777, "place2")
            + (2.1804738294738, 41.405647879212, "place3")
        )
        assert r.geoadd("a", values, nx=True) == 1
        assert r.zrange("a", 0, -1) == [b"place3", b"place2", b"place1"]

    @skip_if_server_version_lt("6.2.0")
    def test_geoadd_xx(self, r):
        values = (2.1909389952632, 41.433791470673, "place1")
        assert r.geoadd("a", values) == 1
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )
        assert r.geoadd("a", values, xx=True) == 0
        assert r.zrange("a", 0, -1) == [b"place1"]

    @skip_if_server_version_lt("6.2.0")
    def test_geoadd_ch(self, r):
        values = (2.1909389952632, 41.433791470673, "place1")
        assert r.geoadd("a", values) == 1
        values = (2.1909389952632, 31.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )
        assert r.geoadd("a", values, ch=True) == 2
        assert r.zrange("a", 0, -1) == [b"place1", b"place2"]

    @skip_if_server_version_lt("3.2.0")
    def test_geoadd_invalid_params(self, r):
        with pytest.raises(exceptions.RedisError):
            r.geoadd("barcelona", (1, 2))

    @skip_if_server_version_lt("3.2.0")
    def test_geodist(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )
        assert r.geoadd("barcelona", values) == 2
        assert r.geodist("barcelona", "place1", "place2") == 3067.4157

    @skip_if_server_version_lt("3.2.0")
    def test_geodist_units(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )
        r.geoadd("barcelona", values)
        assert r.geodist("barcelona", "place1", "place2", "km") == 3.0674

    @skip_if_server_version_lt("3.2.0")
    def test_geodist_missing_one_member(self, r):
        values = (2.1909389952632, 41.433791470673, "place1")
        r.geoadd("barcelona", values)
        assert r.geodist("barcelona", "place1", "missing_member", "km") is None

    @skip_if_server_version_lt("3.2.0")
    def test_geodist_invalid_units(self, r):
        with pytest.raises(exceptions.RedisError):
            assert r.geodist("x", "y", "z", "inches")

    @skip_if_server_version_lt("3.2.0")
    def test_geohash(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )
        r.geoadd("barcelona", values)
        assert r.geohash("barcelona", "place1", "place2", "place3") == [
            "sp3e9yg3kd0",
            "sp3e9cbc3t0",
            None,
        ]

    @skip_unless_arch_bits(64)
    @skip_if_server_version_lt("3.2.0")
    def test_geopos(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )
        r.geoadd("barcelona", values)
        # redis uses 52 bits precision, hereby small errors may be introduced.
        assert r.geopos("barcelona", "place1", "place2") == [
            (2.19093829393386841, 41.43379028184083523),
            (2.18737632036209106, 41.40634178640635099),
        ]

    @skip_if_server_version_lt("4.0.0")
    def test_geopos_no_value(self, r):
        assert r.geopos("barcelona", "place1", "place2") == [None, None]

    @skip_if_server_version_lt("3.2.0")
    @skip_if_server_version_gte("4.0.0")
    def test_old_geopos_no_value(self, r):
        assert r.geopos("barcelona", "place1", "place2") == []

    @skip_if_server_version_lt("6.2.0")
    def test_geosearch(self, r):
        values = (
            (2.1909389952632, 41.433791470673, "place1")
            + (2.1873744593677, 41.406342043777, b"\x80place2")
            + (2.583333, 41.316667, "place3")
        )
        r.geoadd("barcelona", values)
        assert r.geosearch(
            "barcelona", longitude=2.191, latitude=41.433, radius=1000
        ) == [b"place1"]
        assert r.geosearch(
            "barcelona", longitude=2.187, latitude=41.406, radius=1000
        ) == [b"\x80place2"]
        assert r.geosearch(
            "barcelona", longitude=2.191, latitude=41.433, height=1000, width=1000
        ) == [b"place1"]
        assert r.geosearch("barcelona", member="place3", radius=100, unit="km") == [
            b"\x80place2",
            b"place1",
            b"place3",
        ]
        # test count
        assert r.geosearch(
            "barcelona", member="place3", radius=100, unit="km", count=2
        ) == [b"place3", b"\x80place2"]
        assert r.geosearch(
            "barcelona", member="place3", radius=100, unit="km", count=1, any=1
        )[0] in [b"place1", b"place3", b"\x80place2"]

    @skip_unless_arch_bits(64)
    @skip_if_server_version_lt("6.2.0")
    def test_geosearch_member(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            b"\x80place2",
        )

        r.geoadd("barcelona", values)
        assert r.geosearch("barcelona", member="place1", radius=4000) == [
            b"\x80place2",
            b"place1",
        ]
        assert r.geosearch("barcelona", member="place1", radius=10) == [b"place1"]

        assert r.geosearch(
            "barcelona",
            member="place1",
            radius=4000,
            withdist=True,
            withcoord=True,
            withhash=True,
        ) == [
            [
                b"\x80place2",
                3067.4157,
                3471609625421029,
                (2.187376320362091, 41.40634178640635),
            ],
            [
                b"place1",
                0.0,
                3471609698139488,
                (2.1909382939338684, 41.433790281840835),
            ],
        ]

    @skip_if_server_version_lt("6.2.0")
    def test_geosearch_sort(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )
        r.geoadd("barcelona", values)
        assert r.geosearch(
            "barcelona", longitude=2.191, latitude=41.433, radius=3000, sort="ASC"
        ) == [b"place1", b"place2"]
        assert r.geosearch(
            "barcelona", longitude=2.191, latitude=41.433, radius=3000, sort="DESC"
        ) == [b"place2", b"place1"]

    @skip_unless_arch_bits(64)
    @skip_if_server_version_lt("6.2.0")
    def test_geosearch_with(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )
        r.geoadd("barcelona", values)

        # test a bunch of combinations to test the parse response
        # function.
        assert r.geosearch(
            "barcelona",
            longitude=2.191,
            latitude=41.433,
            radius=1,
            unit="km",
            withdist=True,
            withcoord=True,
            withhash=True,
        ) == [
            [
                b"place1",
                0.0881,
                3471609698139488,
                (2.19093829393386841, 41.43379028184083523),
            ]
        ]
        assert r.geosearch(
            "barcelona",
            longitude=2.191,
            latitude=41.433,
            radius=1,
            unit="km",
            withdist=True,
            withcoord=True,
        ) == [[b"place1", 0.0881, (2.19093829393386841, 41.43379028184083523)]]
        assert r.geosearch(
            "barcelona",
            longitude=2.191,
            latitude=41.433,
            radius=1,
            unit="km",
            withhash=True,
            withcoord=True,
        ) == [
            [b"place1", 3471609698139488, (2.19093829393386841, 41.43379028184083523)]
        ]
        # test no values.
        assert (
            r.geosearch(
                "barcelona",
                longitude=2,
                latitude=1,
                radius=1,
                unit="km",
                withdist=True,
                withcoord=True,
                withhash=True,
            )
            == []
        )

    @skip_if_server_version_lt("6.2.0")
    def test_geosearch_negative(self, r):
        # not specifying member nor longitude and latitude
        with pytest.raises(exceptions.DataError):
            assert r.geosearch("barcelona")
        # specifying member and longitude and latitude
        with pytest.raises(exceptions.DataError):
            assert r.geosearch("barcelona", member="Paris", longitude=2, latitude=1)
        # specifying one of longitude and latitude
        with pytest.raises(exceptions.DataError):
            assert r.geosearch("barcelona", longitude=2)
        with pytest.raises(exceptions.DataError):
            assert r.geosearch("barcelona", latitude=2)

        # not specifying radius nor width and height
        with pytest.raises(exceptions.DataError):
            assert r.geosearch("barcelona", member="Paris")
        # specifying radius and width and height
        with pytest.raises(exceptions.DataError):
            assert r.geosearch("barcelona", member="Paris", radius=3, width=2, height=1)
        # specifying one of width and height
        with pytest.raises(exceptions.DataError):
            assert r.geosearch("barcelona", member="Paris", width=2)
        with pytest.raises(exceptions.DataError):
            assert r.geosearch("barcelona", member="Paris", height=2)

        # invalid sort
        with pytest.raises(exceptions.DataError):
            assert r.geosearch(
                "barcelona", member="Paris", width=2, height=2, sort="wrong"
            )

        # invalid unit
        with pytest.raises(exceptions.DataError):
            assert r.geosearch(
                "barcelona", member="Paris", width=2, height=2, unit="miles"
            )

        # use any without count
        with pytest.raises(exceptions.DataError):
            assert r.geosearch("barcelona", member="place3", radius=100, any=1)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    def test_geosearchstore(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        r.geoadd("barcelona", values)
        r.geosearchstore(
            "places_barcelona",
            "barcelona",
            longitude=2.191,
            latitude=41.433,
            radius=1000,
        )
        assert r.zrange("places_barcelona", 0, -1) == [b"place1"]

    @pytest.mark.onlynoncluster
    @skip_unless_arch_bits(64)
    @skip_if_server_version_lt("6.2.0")
    def test_geosearchstore_dist(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        r.geoadd("barcelona", values)
        r.geosearchstore(
            "places_barcelona",
            "barcelona",
            longitude=2.191,
            latitude=41.433,
            radius=1000,
            storedist=True,
        )
        # instead of save the geo score, the distance is saved.
        assert r.zscore("places_barcelona", "place1") == 88.05060698409301

    @skip_if_server_version_lt("3.2.0")
    def test_georadius(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            b"\x80place2",
        )

        r.geoadd("barcelona", values)
        assert r.georadius("barcelona", 2.191, 41.433, 1000) == [b"place1"]
        assert r.georadius("barcelona", 2.187, 41.406, 1000) == [b"\x80place2"]

    @skip_if_server_version_lt("3.2.0")
    def test_georadius_no_values(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        r.geoadd("barcelona", values)
        assert r.georadius("barcelona", 1, 2, 1000) == []

    @skip_if_server_version_lt("3.2.0")
    def test_georadius_units(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        r.geoadd("barcelona", values)
        assert r.georadius("barcelona", 2.191, 41.433, 1, unit="km") == [b"place1"]

    @skip_unless_arch_bits(64)
    @skip_if_server_version_lt("3.2.0")
    def test_georadius_with(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        r.geoadd("barcelona", values)

        # test a bunch of combinations to test the parse response
        # function.
        assert r.georadius(
            "barcelona",
            2.191,
            41.433,
            1,
            unit="km",
            withdist=True,
            withcoord=True,
            withhash=True,
        ) == [
            [
                b"place1",
                0.0881,
                3471609698139488,
                (2.19093829393386841, 41.43379028184083523),
            ]
        ]

        assert r.georadius(
            "barcelona", 2.191, 41.433, 1, unit="km", withdist=True, withcoord=True
        ) == [[b"place1", 0.0881, (2.19093829393386841, 41.43379028184083523)]]

        assert r.georadius(
            "barcelona", 2.191, 41.433, 1, unit="km", withhash=True, withcoord=True
        ) == [
            [b"place1", 3471609698139488, (2.19093829393386841, 41.43379028184083523)]
        ]

        # test no values.
        assert (
            r.georadius(
                "barcelona",
                2,
                1,
                1,
                unit="km",
                withdist=True,
                withcoord=True,
                withhash=True,
            )
            == []
        )

    @skip_if_server_version_lt("6.2.0")
    def test_georadius_count(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        r.geoadd("barcelona", values)
        assert r.georadius("barcelona", 2.191, 41.433, 3000, count=1) == [b"place1"]
        assert r.georadius("barcelona", 2.191, 41.433, 3000, count=1, any=True) == [
            b"place2"
        ]

    @skip_if_server_version_lt("3.2.0")
    def test_georadius_sort(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        r.geoadd("barcelona", values)
        assert r.georadius("barcelona", 2.191, 41.433, 3000, sort="ASC") == [
            b"place1",
            b"place2",
        ]
        assert r.georadius("barcelona", 2.191, 41.433, 3000, sort="DESC") == [
            b"place2",
            b"place1",
        ]

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("3.2.0")
    def test_georadius_store(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        r.geoadd("barcelona", values)
        r.georadius("barcelona", 2.191, 41.433, 1000, store="places_barcelona")
        assert r.zrange("places_barcelona", 0, -1) == [b"place1"]

    @pytest.mark.onlynoncluster
    @skip_unless_arch_bits(64)
    @skip_if_server_version_lt("3.2.0")
    def test_georadius_store_dist(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        r.geoadd("barcelona", values)
        r.georadius("barcelona", 2.191, 41.433, 1000, store_dist="places_barcelona")
        # instead of save the geo score, the distance is saved.
        assert r.zscore("places_barcelona", "place1") == 88.05060698409301

    @skip_unless_arch_bits(64)
    @skip_if_server_version_lt("3.2.0")
    def test_georadiusmember(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            b"\x80place2",
        )

        r.geoadd("barcelona", values)
        assert r.georadiusbymember("barcelona", "place1", 4000) == [
            b"\x80place2",
            b"place1",
        ]
        assert r.georadiusbymember("barcelona", "place1", 10) == [b"place1"]

        assert r.georadiusbymember(
            "barcelona", "place1", 4000, withdist=True, withcoord=True, withhash=True
        ) == [
            [
                b"\x80place2",
                3067.4157,
                3471609625421029,
                (2.187376320362091, 41.40634178640635),
            ],
            [
                b"place1",
                0.0,
                3471609698139488,
                (2.1909382939338684, 41.433790281840835),
            ],
        ]

    @skip_if_server_version_lt("6.2.0")
    def test_georadiusmember_count(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            b"\x80place2",
        )
        r.geoadd("barcelona", values)
        assert r.georadiusbymember("barcelona", "place1", 4000, count=1, any=True) == [
            b"\x80place2"
        ]

    @skip_if_server_version_lt("5.0.0")
    def test_xack(self, r):
        stream = "stream"
        group = "group"
        consumer = "consumer"
        # xack on a stream that doesn't exist
        assert r.xack(stream, group, "0-0") == 0

        m1 = r.xadd(stream, {"one": "one"})
        m2 = r.xadd(stream, {"two": "two"})
        m3 = r.xadd(stream, {"three": "three"})

        # xack on a group that doesn't exist
        assert r.xack(stream, group, m1) == 0

        r.xgroup_create(stream, group, 0)
        r.xreadgroup(group, consumer, streams={stream: ">"})
        # xack returns the number of ack'd elements
        assert r.xack(stream, group, m1) == 1
        assert r.xack(stream, group, m2, m3) == 2

    @skip_if_server_version_lt("5.0.0")
    def test_xadd(self, r):
        stream = "stream"
        message_id = r.xadd(stream, {"foo": "bar"})
        assert re.match(rb"[0-9]+\-[0-9]+", message_id)

        # explicit message id
        message_id = b"9999999999999999999-0"
        assert message_id == r.xadd(stream, {"foo": "bar"}, id=message_id)

        # with maxlen, the list evicts the first message
        r.xadd(stream, {"foo": "bar"}, maxlen=2, approximate=False)
        assert r.xlen(stream) == 2

    @skip_if_server_version_lt("6.2.0")
    def test_xadd_nomkstream(self, r):
        # nomkstream option
        stream = "stream"
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"some": "other"}, nomkstream=False)
        assert r.xlen(stream) == 2
        r.xadd(stream, {"some": "other"}, nomkstream=True)
        assert r.xlen(stream) == 3

    @skip_if_server_version_lt("6.2.0")
    def test_xadd_minlen_and_limit(self, r):
        stream = "stream"

        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})

        # Future self: No limits without approximate, according to the api
        with pytest.raises(redis.ResponseError):
            assert r.xadd(stream, {"foo": "bar"}, maxlen=3, approximate=False, limit=2)

        # limit can not be provided without maxlen or minid
        with pytest.raises(redis.ResponseError):
            assert r.xadd(stream, {"foo": "bar"}, limit=2)

        # maxlen with a limit
        assert r.xadd(stream, {"foo": "bar"}, maxlen=3, approximate=True, limit=2)
        r.delete(stream)

        # maxlen and minid can not be provided together
        with pytest.raises(redis.DataError):
            assert r.xadd(stream, {"foo": "bar"}, maxlen=3, minid="sometestvalue")

        # minid with a limit
        m1 = r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        assert r.xadd(stream, {"foo": "bar"}, approximate=True, minid=m1, limit=3)

        # pure minid
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        m4 = r.xadd(stream, {"foo": "bar"})
        assert r.xadd(stream, {"foo": "bar"}, approximate=False, minid=m4)

        # minid approximate
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        m3 = r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        assert r.xadd(stream, {"foo": "bar"}, approximate=True, minid=m3)

    @skip_if_server_version_lt("7.0.0")
    def test_xadd_explicit_ms(self, r: redis.Redis):
        stream = "stream"
        message_id = r.xadd(stream, {"foo": "bar"}, "9999999999999999999-*")
        ms = message_id[: message_id.index(b"-")]
        assert ms == b"9999999999999999999"

    @skip_if_server_version_lt("6.2.0")
    def test_xautoclaim(self, r):
        stream = "stream"
        group = "group"
        consumer1 = "consumer1"
        consumer2 = "consumer2"

        message_id1 = r.xadd(stream, {"john": "wick"})
        message_id2 = r.xadd(stream, {"johny": "deff"})
        message = get_stream_message(r, stream, message_id1)
        r.xgroup_create(stream, group, 0)

        # trying to claim a message that isn't already pending doesn't
        # do anything
        response = r.xautoclaim(stream, group, consumer2, min_idle_time=0)
        assert response == [b"0-0", []]

        # read the group as consumer1 to initially claim the messages
        r.xreadgroup(group, consumer1, streams={stream: ">"})

        # claim one message as consumer2
        response = r.xautoclaim(stream, group, consumer2, min_idle_time=0, count=1)
        assert response[1] == [message]

        # reclaim the messages as consumer1, but use the justid argument
        # which only returns message ids
        assert r.xautoclaim(
            stream, group, consumer1, min_idle_time=0, start_id=0, justid=True
        ) == [message_id1, message_id2]
        assert r.xautoclaim(
            stream, group, consumer1, min_idle_time=0, start_id=message_id2, justid=True
        ) == [message_id2]

    @skip_if_server_version_lt("6.2.0")
    def test_xautoclaim_negative(self, r):
        stream = "stream"
        group = "group"
        consumer = "consumer"
        with pytest.raises(redis.DataError):
            r.xautoclaim(stream, group, consumer, min_idle_time=-1)
        with pytest.raises(ValueError):
            r.xautoclaim(stream, group, consumer, min_idle_time="wrong")
        with pytest.raises(redis.DataError):
            r.xautoclaim(stream, group, consumer, min_idle_time=0, count=-1)

    @skip_if_server_version_lt("5.0.0")
    def test_xclaim(self, r):
        stream = "stream"
        group = "group"
        consumer1 = "consumer1"
        consumer2 = "consumer2"
        message_id = r.xadd(stream, {"john": "wick"})
        message = get_stream_message(r, stream, message_id)
        r.xgroup_create(stream, group, 0)

        # trying to claim a message that isn't already pending doesn't
        # do anything
        response = r.xclaim(
            stream, group, consumer2, min_idle_time=0, message_ids=(message_id,)
        )
        assert response == []

        # read the group as consumer1 to initially claim the messages
        r.xreadgroup(group, consumer1, streams={stream: ">"})

        # claim the message as consumer2
        response = r.xclaim(
            stream, group, consumer2, min_idle_time=0, message_ids=(message_id,)
        )
        assert response[0] == message

        # reclaim the message as consumer1, but use the justid argument
        # which only returns message ids
        assert r.xclaim(
            stream,
            group,
            consumer1,
            min_idle_time=0,
            message_ids=(message_id,),
            justid=True,
        ) == [message_id]

    @skip_if_server_version_lt("7.0.0")
    def test_xclaim_trimmed(self, r):
        # xclaim should not raise an exception if the item is not there
        stream = "stream"
        group = "group"

        r.xgroup_create(stream, group, id="$", mkstream=True)

        # add a couple of new items
        sid1 = r.xadd(stream, {"item": 0})
        sid2 = r.xadd(stream, {"item": 0})

        # read them from consumer1
        r.xreadgroup(group, "consumer1", {stream: ">"})

        # add a 3rd and trim the stream down to 2 items
        r.xadd(stream, {"item": 3}, maxlen=2, approximate=False)

        # xclaim them from consumer2
        # the item that is still in the stream should be returned
        item = r.xclaim(stream, group, "consumer2", 0, [sid1, sid2])
        assert len(item) == 1
        assert item[0][0] == sid2

    @skip_if_server_version_lt("5.0.0")
    def test_xdel(self, r):
        stream = "stream"

        # deleting from an empty stream doesn't do anything
        assert r.xdel(stream, 1) == 0

        m1 = r.xadd(stream, {"foo": "bar"})
        m2 = r.xadd(stream, {"foo": "bar"})
        m3 = r.xadd(stream, {"foo": "bar"})

        # xdel returns the number of deleted elements
        assert r.xdel(stream, m1) == 1
        assert r.xdel(stream, m2, m3) == 2

    @skip_if_server_version_lt("7.0.0")
    def test_xgroup_create(self, r):
        # tests xgroup_create and xinfo_groups
        stream = "stream"
        group = "group"
        r.xadd(stream, {"foo": "bar"})

        # no group is setup yet, no info to obtain
        assert r.xinfo_groups(stream) == []

        assert r.xgroup_create(stream, group, 0)
        expected = [
            {
                "name": group.encode(),
                "consumers": 0,
                "pending": 0,
                "last-delivered-id": b"0-0",
                "entries-read": None,
                "lag": 1,
            }
        ]
        assert r.xinfo_groups(stream) == expected

    @skip_if_server_version_lt("7.0.0")
    def test_xgroup_create_mkstream(self, r):
        # tests xgroup_create and xinfo_groups
        stream = "stream"
        group = "group"

        # an error is raised if a group is created on a stream that
        # doesn't already exist
        with pytest.raises(exceptions.ResponseError):
            r.xgroup_create(stream, group, 0)

        # however, with mkstream=True, the underlying stream is created
        # automatically
        assert r.xgroup_create(stream, group, 0, mkstream=True)
        expected = [
            {
                "name": group.encode(),
                "consumers": 0,
                "pending": 0,
                "last-delivered-id": b"0-0",
                "entries-read": None,
                "lag": 0,
            }
        ]
        assert r.xinfo_groups(stream) == expected

    @skip_if_server_version_lt("7.0.0")
    def test_xgroup_create_entriesread(self, r: redis.Redis):
        stream = "stream"
        group = "group"
        r.xadd(stream, {"foo": "bar"})

        # no group is setup yet, no info to obtain
        assert r.xinfo_groups(stream) == []

        assert r.xgroup_create(stream, group, 0, entries_read=7)
        expected = [
            {
                "name": group.encode(),
                "consumers": 0,
                "pending": 0,
                "last-delivered-id": b"0-0",
                "entries-read": 7,
                "lag": -6,
            }
        ]
        assert r.xinfo_groups(stream) == expected

    @skip_if_server_version_lt("5.0.0")
    def test_xgroup_delconsumer(self, r):
        stream = "stream"
        group = "group"
        consumer = "consumer"
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xgroup_create(stream, group, 0)

        # a consumer that hasn't yet read any messages doesn't do anything
        assert r.xgroup_delconsumer(stream, group, consumer) == 0

        # read all messages from the group
        r.xreadgroup(group, consumer, streams={stream: ">"})

        # deleting the consumer should return 2 pending messages
        assert r.xgroup_delconsumer(stream, group, consumer) == 2

    @skip_if_server_version_lt("6.2.0")
    def test_xgroup_createconsumer(self, r):
        stream = "stream"
        group = "group"
        consumer = "consumer"
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xgroup_create(stream, group, 0)
        assert r.xgroup_createconsumer(stream, group, consumer) == 1

        # read all messages from the group
        r.xreadgroup(group, consumer, streams={stream: ">"})

        # deleting the consumer should return 2 pending messages
        assert r.xgroup_delconsumer(stream, group, consumer) == 2

    @skip_if_server_version_lt("5.0.0")
    def test_xgroup_destroy(self, r):
        stream = "stream"
        group = "group"
        r.xadd(stream, {"foo": "bar"})

        # destroying a nonexistent group returns False
        assert not r.xgroup_destroy(stream, group)

        r.xgroup_create(stream, group, 0)
        assert r.xgroup_destroy(stream, group)

    @skip_if_server_version_lt("7.0.0")
    def test_xgroup_setid(self, r):
        stream = "stream"
        group = "group"
        message_id = r.xadd(stream, {"foo": "bar"})

        r.xgroup_create(stream, group, 0)
        # advance the last_delivered_id to the message_id
        r.xgroup_setid(stream, group, message_id, entries_read=2)
        expected = [
            {
                "name": group.encode(),
                "consumers": 0,
                "pending": 0,
                "last-delivered-id": message_id,
                "entries-read": 2,
                "lag": -1,
            }
        ]
        assert r.xinfo_groups(stream) == expected

    @skip_if_server_version_lt("5.0.0")
    def test_xinfo_consumers(self, r):
        stream = "stream"
        group = "group"
        consumer1 = "consumer1"
        consumer2 = "consumer2"
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})

        r.xgroup_create(stream, group, 0)
        r.xreadgroup(group, consumer1, streams={stream: ">"}, count=1)
        r.xreadgroup(group, consumer2, streams={stream: ">"})
        info = r.xinfo_consumers(stream, group)
        assert len(info) == 2
        expected = [
            {"name": consumer1.encode(), "pending": 1},
            {"name": consumer2.encode(), "pending": 2},
        ]

        # we can't determine the idle time, so just make sure it's an int
        assert isinstance(info[0].pop("idle"), int)
        assert isinstance(info[1].pop("idle"), int)
        assert info == expected

    @skip_if_server_version_lt("7.0.0")
    def test_xinfo_stream(self, r):
        stream = "stream"
        m1 = r.xadd(stream, {"foo": "bar"})
        m2 = r.xadd(stream, {"foo": "bar"})
        info = r.xinfo_stream(stream)

        assert info["length"] == 2
        assert info["first-entry"] == get_stream_message(r, stream, m1)
        assert info["last-entry"] == get_stream_message(r, stream, m2)
        assert info["max-deleted-entry-id"] == b"0-0"
        assert info["entries-added"] == 2
        assert info["recorded-first-entry-id"] == m1

    @skip_if_server_version_lt("6.0.0")
    def test_xinfo_stream_full(self, r):
        stream = "stream"
        group = "group"
        m1 = r.xadd(stream, {"foo": "bar"})
        r.xgroup_create(stream, group, 0)
        info = r.xinfo_stream(stream, full=True)

        assert info["length"] == 1
        assert m1 in info["entries"]
        assert len(info["groups"]) == 1

    @skip_if_server_version_lt("5.0.0")
    def test_xlen(self, r):
        stream = "stream"
        assert r.xlen(stream) == 0
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        assert r.xlen(stream) == 2

    @skip_if_server_version_lt("5.0.0")
    def test_xpending(self, r):
        stream = "stream"
        group = "group"
        consumer1 = "consumer1"
        consumer2 = "consumer2"
        m1 = r.xadd(stream, {"foo": "bar"})
        m2 = r.xadd(stream, {"foo": "bar"})
        r.xgroup_create(stream, group, 0)

        # xpending on a group that has no consumers yet
        expected = {"pending": 0, "min": None, "max": None, "consumers": []}
        assert r.xpending(stream, group) == expected

        # read 1 message from the group with each consumer
        r.xreadgroup(group, consumer1, streams={stream: ">"}, count=1)
        r.xreadgroup(group, consumer2, streams={stream: ">"}, count=1)

        expected = {
            "pending": 2,
            "min": m1,
            "max": m2,
            "consumers": [
                {"name": consumer1.encode(), "pending": 1},
                {"name": consumer2.encode(), "pending": 1},
            ],
        }
        assert r.xpending(stream, group) == expected

    @skip_if_server_version_lt("5.0.0")
    def test_xpending_range(self, r):
        stream = "stream"
        group = "group"
        consumer1 = "consumer1"
        consumer2 = "consumer2"
        m1 = r.xadd(stream, {"foo": "bar"})
        m2 = r.xadd(stream, {"foo": "bar"})
        r.xgroup_create(stream, group, 0)

        # xpending range on a group that has no consumers yet
        assert r.xpending_range(stream, group, min="-", max="+", count=5) == []

        # read 1 message from the group with each consumer
        r.xreadgroup(group, consumer1, streams={stream: ">"}, count=1)
        r.xreadgroup(group, consumer2, streams={stream: ">"}, count=1)

        response = r.xpending_range(stream, group, min="-", max="+", count=5)
        assert len(response) == 2
        assert response[0]["message_id"] == m1
        assert response[0]["consumer"] == consumer1.encode()
        assert response[1]["message_id"] == m2
        assert response[1]["consumer"] == consumer2.encode()

        # test with consumer name
        response = r.xpending_range(
            stream, group, min="-", max="+", count=5, consumername=consumer1
        )
        assert response[0]["message_id"] == m1
        assert response[0]["consumer"] == consumer1.encode()

    @skip_if_server_version_lt("6.2.0")
    def test_xpending_range_idle(self, r):
        stream = "stream"
        group = "group"
        consumer1 = "consumer1"
        consumer2 = "consumer2"
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xgroup_create(stream, group, 0)

        # read 1 message from the group with each consumer
        r.xreadgroup(group, consumer1, streams={stream: ">"}, count=1)
        r.xreadgroup(group, consumer2, streams={stream: ">"}, count=1)

        response = r.xpending_range(stream, group, min="-", max="+", count=5)
        assert len(response) == 2
        response = r.xpending_range(stream, group, min="-", max="+", count=5, idle=1000)
        assert len(response) == 0

    def test_xpending_range_negative(self, r):
        stream = "stream"
        group = "group"
        with pytest.raises(redis.DataError):
            r.xpending_range(stream, group, min="-", max="+", count=None)
        with pytest.raises(ValueError):
            r.xpending_range(stream, group, min="-", max="+", count="one")
        with pytest.raises(redis.DataError):
            r.xpending_range(stream, group, min="-", max="+", count=-1)
        with pytest.raises(ValueError):
            r.xpending_range(stream, group, min="-", max="+", count=5, idle="one")
        with pytest.raises(redis.exceptions.ResponseError):
            r.xpending_range(stream, group, min="-", max="+", count=5, idle=1.5)
        with pytest.raises(redis.DataError):
            r.xpending_range(stream, group, min="-", max="+", count=5, idle=-1)
        with pytest.raises(redis.DataError):
            r.xpending_range(stream, group, min=None, max=None, count=None, idle=0)
        with pytest.raises(redis.DataError):
            r.xpending_range(
                stream, group, min=None, max=None, count=None, consumername=0
            )

    @skip_if_server_version_lt("5.0.0")
    def test_xrange(self, r):
        stream = "stream"
        m1 = r.xadd(stream, {"foo": "bar"})
        m2 = r.xadd(stream, {"foo": "bar"})
        m3 = r.xadd(stream, {"foo": "bar"})
        m4 = r.xadd(stream, {"foo": "bar"})

        def get_ids(results):
            return [result[0] for result in results]

        results = r.xrange(stream, min=m1)
        assert get_ids(results) == [m1, m2, m3, m4]

        results = r.xrange(stream, min=m2, max=m3)
        assert get_ids(results) == [m2, m3]

        results = r.xrange(stream, max=m3)
        assert get_ids(results) == [m1, m2, m3]

        results = r.xrange(stream, max=m2, count=1)
        assert get_ids(results) == [m1]

    @skip_if_server_version_lt("5.0.0")
    def test_xread(self, r):
        stream = "stream"
        m1 = r.xadd(stream, {"foo": "bar"})
        m2 = r.xadd(stream, {"bing": "baz"})

        expected = [
            [
                stream.encode(),
                [get_stream_message(r, stream, m1), get_stream_message(r, stream, m2)],
            ]
        ]
        # xread starting at 0 returns both messages
        assert r.xread(streams={stream: 0}) == expected

        expected = [[stream.encode(), [get_stream_message(r, stream, m1)]]]
        # xread starting at 0 and count=1 returns only the first message
        assert r.xread(streams={stream: 0}, count=1) == expected

        expected = [[stream.encode(), [get_stream_message(r, stream, m2)]]]
        # xread starting at m1 returns only the second message
        assert r.xread(streams={stream: m1}) == expected

        # xread starting at the last message returns an empty list
        assert r.xread(streams={stream: m2}) == []

    @skip_if_server_version_lt("5.0.0")
    def test_xreadgroup(self, r):
        stream = "stream"
        group = "group"
        consumer = "consumer"
        m1 = r.xadd(stream, {"foo": "bar"})
        m2 = r.xadd(stream, {"bing": "baz"})
        r.xgroup_create(stream, group, 0)

        expected = [
            [
                stream.encode(),
                [get_stream_message(r, stream, m1), get_stream_message(r, stream, m2)],
            ]
        ]
        # xread starting at 0 returns both messages
        assert r.xreadgroup(group, consumer, streams={stream: ">"}) == expected

        r.xgroup_destroy(stream, group)
        r.xgroup_create(stream, group, 0)

        expected = [[stream.encode(), [get_stream_message(r, stream, m1)]]]
        # xread with count=1 returns only the first message
        assert r.xreadgroup(group, consumer, streams={stream: ">"}, count=1) == expected

        r.xgroup_destroy(stream, group)

        # create the group using $ as the last id meaning subsequent reads
        # will only find messages added after this
        r.xgroup_create(stream, group, "$")

        expected = []
        # xread starting after the last message returns an empty message list
        assert r.xreadgroup(group, consumer, streams={stream: ">"}) == expected

        # xreadgroup with noack does not have any items in the PEL
        r.xgroup_destroy(stream, group)
        r.xgroup_create(stream, group, "0")
        assert (
            len(r.xreadgroup(group, consumer, streams={stream: ">"}, noack=True)[0][1])
            == 2
        )
        # now there should be nothing pending
        assert len(r.xreadgroup(group, consumer, streams={stream: "0"})[0][1]) == 0

        r.xgroup_destroy(stream, group)
        r.xgroup_create(stream, group, "0")
        # delete all the messages in the stream
        expected = [[stream.encode(), [(m1, {}), (m2, {})]]]
        r.xreadgroup(group, consumer, streams={stream: ">"})
        r.xtrim(stream, 0)
        assert r.xreadgroup(group, consumer, streams={stream: "0"}) == expected

    @skip_if_server_version_lt("5.0.0")
    def test_xrevrange(self, r):
        stream = "stream"
        m1 = r.xadd(stream, {"foo": "bar"})
        m2 = r.xadd(stream, {"foo": "bar"})
        m3 = r.xadd(stream, {"foo": "bar"})
        m4 = r.xadd(stream, {"foo": "bar"})

        def get_ids(results):
            return [result[0] for result in results]

        results = r.xrevrange(stream, max=m4)
        assert get_ids(results) == [m4, m3, m2, m1]

        results = r.xrevrange(stream, max=m3, min=m2)
        assert get_ids(results) == [m3, m2]

        results = r.xrevrange(stream, min=m3)
        assert get_ids(results) == [m4, m3]

        results = r.xrevrange(stream, min=m2, count=1)
        assert get_ids(results) == [m4]

    @skip_if_server_version_lt("5.0.0")
    def test_xtrim(self, r):
        stream = "stream"

        # trimming an empty key doesn't do anything
        assert r.xtrim(stream, 1000) == 0

        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})

        # trimming an amount large than the number of messages
        # doesn't do anything
        assert r.xtrim(stream, 5, approximate=False) == 0

        # 1 message is trimmed
        assert r.xtrim(stream, 3, approximate=False) == 1

    @skip_if_server_version_lt("6.2.4")
    def test_xtrim_minlen_and_length_args(self, r):
        stream = "stream"

        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})

        # Future self: No limits without approximate, according to the api
        with pytest.raises(redis.ResponseError):
            assert r.xtrim(stream, 3, approximate=False, limit=2)

        # maxlen with a limit
        assert r.xtrim(stream, 3, approximate=True, limit=2) == 0
        r.delete(stream)

        with pytest.raises(redis.DataError):
            assert r.xtrim(stream, maxlen=3, minid="sometestvalue")

        # minid with a limit
        m1 = r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        assert r.xtrim(stream, None, approximate=True, minid=m1, limit=3) == 0

        # pure minid
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        m4 = r.xadd(stream, {"foo": "bar"})
        assert r.xtrim(stream, None, approximate=False, minid=m4) == 7

        # minid approximate
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        m3 = r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        assert r.xtrim(stream, None, approximate=True, minid=m3) == 0

    def test_bitfield_operations(self, r):
        # comments show affected bits
        bf = r.bitfield("a")
        resp = (
            bf.set("u8", 8, 255)  # 00000000 11111111
            .get("u8", 0)  # 00000000
            .get("u4", 8)  # 1111
            .get("u4", 12)  # 1111
            .get("u4", 13)  # 111 0
            .execute()
        )
        assert resp == [0, 0, 15, 15, 14]

        # .set() returns the previous value...
        resp = (
            bf.set("u8", 4, 1)  # 0000 0001
            .get("u16", 0)  # 00000000 00011111
            .set("u16", 0, 0)  # 00000000 00000000
            .execute()
        )
        assert resp == [15, 31, 31]

        # incrby adds to the value
        resp = (
            bf.incrby("u8", 8, 254)  # 00000000 11111110
            .incrby("u8", 8, 1)  # 00000000 11111111
            .get("u16", 0)  # 00000000 11111111
            .execute()
        )
        assert resp == [254, 255, 255]

        # Verify overflow protection works as a method:
        r.delete("a")
        resp = (
            bf.set("u8", 8, 254)  # 00000000 11111110
            .overflow("fail")
            .incrby("u8", 8, 2)  # incrby 2 would overflow, None returned
            .incrby("u8", 8, 1)  # 00000000 11111111
            .incrby("u8", 8, 1)  # incrby 1 would overflow, None returned
            .get("u16", 0)  # 00000000 11111111
            .execute()
        )
        assert resp == [0, None, 255, None, 255]

        # Verify overflow protection works as arg to incrby:
        r.delete("a")
        resp = (
            bf.set("u8", 8, 255)  # 00000000 11111111
            .incrby("u8", 8, 1)  # 00000000 00000000  wrap default
            .set("u8", 8, 255)  # 00000000 11111111
            .incrby("u8", 8, 1, "FAIL")  # 00000000 11111111  fail
            .incrby("u8", 8, 1)  # 00000000 11111111  still fail
            .get("u16", 0)  # 00000000 11111111
            .execute()
        )
        assert resp == [0, 0, 0, None, None, 255]

        # test default default_overflow
        r.delete("a")
        bf = r.bitfield("a", default_overflow="FAIL")
        resp = (
            bf.set("u8", 8, 255)  # 00000000 11111111
            .incrby("u8", 8, 1)  # 00000000 11111111  fail default
            .get("u16", 0)  # 00000000 11111111
            .execute()
        )
        assert resp == [0, None, 255]

    @skip_if_server_version_lt("4.0.0")
    def test_memory_help(self, r):
        with pytest.raises(NotImplementedError):
            r.memory_help()

    @skip_if_server_version_lt("4.0.0")
    def test_memory_doctor(self, r):
        with pytest.raises(NotImplementedError):
            r.memory_doctor()

    @skip_if_server_version_lt("4.0.0")
    @skip_if_redis_enterprise()
    def test_memory_malloc_stats(self, r):
        if skip_if_redis_enterprise().args[0] is True:
            with pytest.raises(redis.exceptions.ResponseError):
                assert r.memory_malloc_stats()
            return

        assert r.memory_malloc_stats()

    @skip_if_server_version_lt("4.0.0")
    @skip_if_redis_enterprise()
    def test_memory_stats(self, r):
        # put a key into the current db to make sure that "db.<current-db>"
        # has data
        r.set("foo", "bar")

        if skip_if_redis_enterprise().args[0] is True:
            with pytest.raises(redis.exceptions.ResponseError):
                stats = r.memory_stats()
            return

        stats = r.memory_stats()
        assert isinstance(stats, dict)
        for key, value in stats.items():
            if key.startswith("db."):
                assert isinstance(value, dict)

    @skip_if_server_version_lt("4.0.0")
    def test_memory_usage(self, r):
        r.set("foo", "bar")
        assert isinstance(r.memory_usage("foo"), int)

    @skip_if_server_version_lt("7.0.0")
    def test_latency_histogram_not_implemented(self, r: redis.Redis):
        with pytest.raises(NotImplementedError):
            r.latency_histogram()

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("4.0.0")
    @skip_if_redis_enterprise()
    def test_module_list(self, r):
        assert isinstance(r.module_list(), list)
        for x in r.module_list():
            assert isinstance(x, dict)

    @skip_if_server_version_lt("2.8.13")
    @skip_if_redis_enterprise()
    def test_command_count(self, r):
        res = r.command_count()
        assert isinstance(res, int)
        assert res >= 100

    @skip_if_server_version_lt("7.0.0")
    def test_command_docs(self, r):
        with pytest.raises(NotImplementedError):
            r.command_docs("set")

    @skip_if_server_version_lt("7.0.0")
    @skip_if_redis_enterprise()
    def test_command_list(self, r: redis.Redis):
        assert len(r.command_list()) > 300
        assert len(r.command_list(module="fakemod")) == 0
        assert len(r.command_list(category="list")) > 15
        assert b"lpop" in r.command_list(pattern="l*")
        with pytest.raises(redis.ResponseError):
            r.command_list(category="list", pattern="l*")

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.13")
    @skip_if_redis_enterprise()
    def test_command_getkeys(self, r):
        res = r.command_getkeys("MSET", "a", "b", "c", "d", "e", "f")
        assert res == ["a", "c", "e"]
        res = r.command_getkeys(
            "EVAL",
            '"not consulted"',
            "3",
            "key1",
            "key2",
            "key3",
            "arg1",
            "arg2",
            "arg3",
            "argN",
        )
        assert res == ["key1", "key2", "key3"]

    @skip_if_server_version_lt("2.8.13")
    def test_command(self, r):
        res = r.command()
        assert len(res) >= 100
        cmds = list(res.keys())
        assert "set" in cmds
        assert "get" in cmds

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    @skip_if_redis_enterprise()
    def test_command_getkeysandflags(self, r: redis.Redis):
        res = [
            [b"mylist1", [b"RW", b"access", b"delete"]],
            [b"mylist2", [b"RW", b"insert"]],
        ]
        assert res == r.command_getkeysandflags(
            "LMOVE", "mylist1", "mylist2", "left", "left"
        )

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("4.0.0")
    @skip_if_redis_enterprise()
    def test_module(self, r):
        with pytest.raises(redis.exceptions.ModuleError) as excinfo:
            r.module_load("/some/fake/path")
            assert "Error loading the extension." in str(excinfo.value)

        with pytest.raises(redis.exceptions.ModuleError) as excinfo:
            r.module_load("/some/fake/path", "arg1", "arg2", "arg3", "arg4")
            assert "Error loading the extension." in str(excinfo.value)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    @skip_if_redis_enterprise()
    def test_module_loadex(self, r: redis.Redis):
        with pytest.raises(redis.exceptions.ModuleError) as excinfo:
            r.module_loadex("/some/fake/path")
            assert "Error loading the extension." in str(excinfo.value)

        with pytest.raises(redis.exceptions.ModuleError) as excinfo:
            r.module_loadex("/some/fake/path", ["name", "value"], ["arg1", "arg2"])
            assert "Error loading the extension." in str(excinfo.value)

    @skip_if_server_version_lt("2.6.0")
    def test_restore(self, r):

        # standard restore
        key = "foo"
        r.set(key, "bar")
        dumpdata = r.dump(key)
        r.delete(key)
        assert r.restore(key, 0, dumpdata)
        assert r.get(key) == b"bar"

        # overwrite restore
        with pytest.raises(redis.exceptions.ResponseError):
            assert r.restore(key, 0, dumpdata)
        r.set(key, "a new value!")
        assert r.restore(key, 0, dumpdata, replace=True)
        assert r.get(key) == b"bar"

        # ttl check
        key2 = "another"
        r.set(key2, "blee!")
        dumpdata = r.dump(key2)
        r.delete(key2)
        assert r.restore(key2, 0, dumpdata)
        assert r.ttl(key2) == -1

    @skip_if_server_version_lt("5.0.0")
    def test_restore_idletime(self, r):
        key = "yayakey"
        r.set(key, "blee!")
        dumpdata = r.dump(key)
        r.delete(key)
        assert r.restore(key, 0, dumpdata, idletime=5)
        assert r.get(key) == b"blee!"

    @skip_if_server_version_lt("5.0.0")
    def test_restore_frequency(self, r):
        key = "yayakey"
        r.set(key, "blee!")
        dumpdata = r.dump(key)
        r.delete(key)
        assert r.restore(key, 0, dumpdata, frequency=5)
        assert r.get(key) == b"blee!"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("5.0.0")
    @skip_if_redis_enterprise()
    def test_replicaof(self, r):
        with pytest.raises(redis.ResponseError):
            assert r.replicaof("NO ONE")
        assert r.replicaof("NO", "ONE")

    def test_shutdown(self, r: redis.Redis):
        r.execute_command = mock.MagicMock()
        r.execute_command("SHUTDOWN", "NOSAVE")
        r.execute_command.assert_called_once_with("SHUTDOWN", "NOSAVE")

    @skip_if_server_version_lt("7.0.0")
    def test_shutdown_with_params(self, r: redis.Redis):
        r.execute_command = mock.MagicMock()
        r.execute_command("SHUTDOWN", "SAVE", "NOW", "FORCE")
        r.execute_command.assert_called_once_with("SHUTDOWN", "SAVE", "NOW", "FORCE")
        r.execute_command("SHUTDOWN", "ABORT")
        r.execute_command.assert_called_with("SHUTDOWN", "ABORT")

    @pytest.mark.replica
    @skip_if_server_version_lt("2.8.0")
    @skip_if_redis_enterprise()
    def test_sync(self, r):
        r2 = redis.Redis(port=6380, decode_responses=False)
        res = r2.sync()
        assert b"REDIS" in res

    @pytest.mark.replica
    @skip_if_server_version_lt("2.8.0")
    @skip_if_redis_enterprise()
    def test_psync(self, r):
        r2 = redis.Redis(port=6380, decode_responses=False)
        res = r2.psync(r2.client_id(), 1)
        assert b"FULLRESYNC" in res

    @pytest.mark.onlynoncluster
    def test_interrupted_command(self, r: redis.Redis):
        """
        Regression test for issue #1128:  An Un-handled BaseException
        will leave the socket with un-read response to a previous
        command.
        """

        ok = False

        def helper():
            with pytest.raises(CancelledError):
                # blocking pop
                with patch.object(
                    r.connection._parser, "read_response", side_effect=CancelledError
                ):
                    r.brpop(["nonexist"])
            # if all is well, we can continue.
            r.set("status", "down")  # should not hang
            nonlocal ok
            ok = True

        thread = threading.Thread(target=helper)
        thread.start()
        thread.join(0.1)
        try:
            assert not thread.is_alive()
            assert ok
        finally:
            # disconnect here so that fixture cleanup can proceed
            r.connection.disconnect()


@pytest.mark.onlynoncluster
class TestBinarySave:
    def test_binary_get_set(self, r):
        assert r.set(" foo bar ", "123")
        assert r.get(" foo bar ") == b"123"

        assert r.set(" foo\r\nbar\r\n ", "456")
        assert r.get(" foo\r\nbar\r\n ") == b"456"

        assert r.set(" \r\n\t\x07\x13 ", "789")
        assert r.get(" \r\n\t\x07\x13 ") == b"789"

        assert sorted(r.keys("*")) == [
            b" \r\n\t\x07\x13 ",
            b" foo\r\nbar\r\n ",
            b" foo bar ",
        ]

        assert r.delete(" foo bar ")
        assert r.delete(" foo\r\nbar\r\n ")
        assert r.delete(" \r\n\t\x07\x13 ")

    def test_binary_lists(self, r):
        mapping = {
            b"foo bar": [b"1", b"2", b"3"],
            b"foo\r\nbar\r\n": [b"4", b"5", b"6"],
            b"foo\tbar\x07": [b"7", b"8", b"9"],
        }
        # fill in lists
        for key, value in mapping.items():
            r.rpush(key, *value)

        # check that KEYS returns all the keys as they are
        assert sorted(r.keys("*")) == sorted(mapping.keys())

        # check that it is possible to get list content by key name
        for key, value in mapping.items():
            assert r.lrange(key, 0, -1) == value

    def test_22_info(self, r):
        """
        Older Redis versions contained 'allocation_stats' in INFO that
        was the cause of a number of bugs when parsing.
        """
        info = (
            "allocation_stats:6=1,7=1,8=7141,9=180,10=92,11=116,12=5330,"
            "13=123,14=3091,15=11048,16=225842,17=1784,18=814,19=12020,"
            "20=2530,21=645,22=15113,23=8695,24=142860,25=318,26=3303,"
            "27=20561,28=54042,29=37390,30=1884,31=18071,32=31367,33=160,"
            "34=169,35=201,36=10155,37=1045,38=15078,39=22985,40=12523,"
            "41=15588,42=265,43=1287,44=142,45=382,46=945,47=426,48=171,"
            "49=56,50=516,51=43,52=41,53=46,54=54,55=75,56=647,57=332,"
            "58=32,59=39,60=48,61=35,62=62,63=32,64=221,65=26,66=30,"
            "67=36,68=41,69=44,70=26,71=144,72=169,73=24,74=37,75=25,"
            "76=42,77=21,78=126,79=374,80=27,81=40,82=43,83=47,84=46,"
            "85=114,86=34,87=37,88=7240,89=34,90=38,91=18,92=99,93=20,"
            "94=18,95=17,96=15,97=22,98=18,99=69,100=17,101=22,102=15,"
            "103=29,104=39,105=30,106=70,107=22,108=21,109=26,110=52,"
            "111=45,112=33,113=67,114=41,115=44,116=48,117=53,118=54,"
            "119=51,120=75,121=44,122=57,123=44,124=66,125=56,126=52,"
            "127=81,128=108,129=70,130=50,131=51,132=53,133=45,134=62,"
            "135=12,136=13,137=7,138=15,139=21,140=11,141=20,142=6,143=7,"
            "144=11,145=6,146=16,147=19,148=1112,149=1,151=83,154=1,"
            "155=1,156=1,157=1,160=1,161=1,162=2,166=1,169=1,170=1,171=2,"
            "172=1,174=1,176=2,177=9,178=34,179=73,180=30,181=1,185=3,"
            "187=1,188=1,189=1,192=1,196=1,198=1,200=1,201=1,204=1,205=1,"
            "207=1,208=1,209=1,214=2,215=31,216=78,217=28,218=5,219=2,"
            "220=1,222=1,225=1,227=1,234=1,242=1,250=1,252=1,253=1,"
            ">=256=203"
        )
        parsed = parse_info(info)
        assert "allocation_stats" in parsed
        assert "6" in parsed["allocation_stats"]
        assert ">=256" in parsed["allocation_stats"]

    @skip_if_redis_enterprise()
    def test_large_responses(self, r):
        "The PythonParser has some special cases for return values > 1MB"
        # load up 5MB of data into a key
        data = "".join([ascii_letters] * (5000000 // len(ascii_letters)))
        r["a"] = data
        assert r["a"] == data.encode()

    def test_floating_point_encoding(self, r):
        """
        High precision floating point values sent to the server should keep
        precision.
        """
        timestamp = 1349673917.939762
        r.zadd("a", {"a1": timestamp})
        assert r.zscore("a", "a1") == timestamp
