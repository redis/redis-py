import pytest
from redis.cluster import LoadBalancer

default_host = "127.0.0.1"


@pytest.mark.onlycluster
class TestLoadBalancer:
    """
    Tests for the LoadBalancer class
    """

    def test_load_balancer(self) -> None:
        lb = LoadBalancer()
        primary1_name = f"{default_host}:6379"
        primary2_name = f"{default_host}:6378"
        list1_size = 3
        list2_size = 2
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

    def test_load_balancer_changed_slot_replicas(self) -> None:
        lb = LoadBalancer()
        primary1_name = f"{default_host}:6379"
        list1_size = 3
        # slot 1
        assert lb.get_server_index(primary1_name, list1_size) == 0
        assert lb.get_server_index(primary1_name, list1_size) == 1
        assert lb.get_server_index(primary1_name, list1_size) == 2
        assert lb.get_server_index(primary1_name, list1_size) == 0
        assert lb.get_server_index(primary1_name, list1_size) == 1

        # adjust lb-size
        list1_size = 2
        # This is wrong. The list size is 2, but that doesn't make sense
        assert lb.get_server_index(primary1_name, list1_size) == 2
        # This is also wrong - we skipped node zero
        assert lb.get_server_index(primary1_name, list1_size) == 1
