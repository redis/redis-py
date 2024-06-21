import pytest
from redis.cluster import ClusterNode, LoadBalancer

default_host = "127.0.0.1"
default_port = 6379


@pytest.mark.onlycluster
class TestLoadBalancer:
    """
    Tests for the LoadBalancer class
    """

    def test_get_server_index(self) -> None:
        lb = LoadBalancer()
        primary1_name = f"{default_host}:{default_port}"
        primary2_name = f"{default_host}:{default_port+1}"
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

    def test_get_server_index_changed_slot_replicas(self) -> None:
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
        assert lb.get_server_index(primary1_name, list1_size) == 0
        assert lb.get_server_index(primary1_name, list1_size) == 1
        assert lb.get_server_index(primary1_name, list1_size) == 0

    def test_get_node_from_slot_single_node_primary_only(self) -> None:
        """
        Test that the load balancer handles a server with only a single primary node
        """
        load_balancer = LoadBalancer()
        slot_nodes = [ClusterNode(default_host, default_port)]
        assert (
            load_balancer.get_node_from_slot(slot_nodes, read_from_replicas=False)
            == slot_nodes[0]
        )

    def test_get_node_from_slot_multiple_node_primary_only(self) -> None:
        """
        Test that the load balancer handles a server with multiple nodes, but only read
        from primaries
        """
        load_balancer = LoadBalancer()
        slot_nodes = [
            ClusterNode(default_host, default_port),
            ClusterNode(default_host, default_port + 1),
        ]
        assert (
            load_balancer.get_node_from_slot(slot_nodes, read_from_replicas=False)
            == slot_nodes[0]
        )
        assert (
            load_balancer.get_node_from_slot(slot_nodes, read_from_replicas=False)
            == slot_nodes[0]
        )

    def test_get_node_from_slot_single_node_read_from_replicas(self) -> None:
        """
        Test that the load balancer handles a server with only primary nodes, but also
        try to read from replicas
        """
        load_balancer = LoadBalancer()
        slot_nodes = [ClusterNode(default_host, default_port)]
        assert (
            load_balancer.get_node_from_slot(slot_nodes, read_from_replicas=True)
            == slot_nodes[0]
        )
        assert (
            load_balancer.get_node_from_slot(slot_nodes, read_from_replicas=True)
            == slot_nodes[0]
        )

    def test_get_node_from_slot_multiple_node_read_from_replicas(self) -> None:
        """
        Test that the load balancer handles a server with primary and replica nodes,
        but also try to read from replicas
        """
        load_balancer = LoadBalancer()
        slot_nodes = [
            ClusterNode(default_host, default_port),
            ClusterNode(default_host, default_port + 1),
            ClusterNode(default_host, default_port + 2),
        ]
        assert (
            load_balancer.get_node_from_slot(slot_nodes, read_from_replicas=True)
            == slot_nodes[0]
        )
        assert (
            load_balancer.get_node_from_slot(slot_nodes, read_from_replicas=True)
            == slot_nodes[1]
        )
        assert (
            load_balancer.get_node_from_slot(slot_nodes, read_from_replicas=True)
            == slot_nodes[2]
        )

    def test_get_node_from_slot_multiple_node_read_from_replicas_resizing(self) -> None:
        """
        Test that the load balancer handles a server with primary and replica nodes,
        but also try to read from replicas, and handles resharding slots
        """
        load_balancer = LoadBalancer()
        slot_nodes = [
            ClusterNode(default_host, default_port),
            ClusterNode(default_host, default_port + 1),
            ClusterNode(default_host, default_port + 2),
        ]
        assert (
            load_balancer.get_node_from_slot(slot_nodes, read_from_replicas=True)
            == slot_nodes[0]
        )
        assert (
            load_balancer.get_node_from_slot(slot_nodes, read_from_replicas=True)
            == slot_nodes[1]
        )
        assert (
            load_balancer.get_node_from_slot(slot_nodes, read_from_replicas=True)
            == slot_nodes[2]
        )
        assert (
            load_balancer.get_node_from_slot(slot_nodes, read_from_replicas=True)
            == slot_nodes[0]
        )
        assert (
            load_balancer.get_node_from_slot(slot_nodes, read_from_replicas=True)
            == slot_nodes[1]
        )

        slot_nodes.pop()
        assert (
            load_balancer.get_node_from_slot(slot_nodes, read_from_replicas=True)
            == slot_nodes[0]
        )
        assert (
            load_balancer.get_node_from_slot(slot_nodes, read_from_replicas=True)
            == slot_nodes[1]
        )
        assert (
            load_balancer.get_node_from_slot(slot_nodes, read_from_replicas=True)
            == slot_nodes[0]
        )
