import redis


class TestClient(object):
    def test_client_equality(self):
        r1 = redis.Redis.from_url('redis://localhost:6379/9')
        r2 = redis.Redis.from_url('redis://localhost:6379/9')
        assert r1 == r2

    def test_clients_unequal_if_different_types(self):
        r = redis.Redis.from_url('redis://localhost:6379/9')
        assert r != 0

    def test_clients_unequal_if_different_hosts(self):
        r1 = redis.Redis.from_url('redis://localhost:6379/9')
        r2 = redis.Redis.from_url('redis://127.0.0.1:6379/9')
        assert r1 != r2

    def test_clients_unequal_if_different_ports(self):
        r1 = redis.Redis.from_url('redis://localhost:6379/9')
        r2 = redis.Redis.from_url('redis://localhost:6380/9')
        assert r1 != r2

    def test_clients_unequal_if_different_dbs(self):
        r1 = redis.Redis.from_url('redis://localhost:6379/9')
        r2 = redis.Redis.from_url('redis://localhost:6380/10')
        assert r1 != r2
